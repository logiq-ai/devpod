package kubernetes

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/klog/v2"

	"github.com/go-logr/logr"
	loftlog "github.com/loft-sh/log"
	"github.com/sirupsen/logrus"
)

var setupKlogOnce sync.Once

type Client struct {
	client *kubernetes.Clientset

	config *rest.Config
}

func NewClient(kubeConfig, kubeContext string) (*Client, error) {
	if kubeConfig == "" {
		kubeConfig = os.Getenv("KUBECONFIG")
	}

	// create client config loading rules
	var clientConfigLoadingRules *clientcmd.ClientConfigLoadingRules
	if kubeConfig != "" {
		clientConfigLoadingRules = &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeConfig}
	} else {
		clientConfigLoadingRules = clientcmd.NewDefaultClientConfigLoadingRules()
	}

	// load kubernetes config
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientConfigLoadingRules,
		&clientcmd.ConfigOverrides{CurrentContext: kubeContext},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load kubernetes config: %w", err)
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: kubeClient,
		config: config,
	}, nil
}

func (c *Client) Client() *kubernetes.Clientset {
	return c.client
}

func (c *Client) Config() *rest.Config {
	return c.config
}

func (c *Client) FullLogs(ctx context.Context, namespace, pod, container string) ([]byte, error) {
	logs, err := c.Logs(ctx, namespace, pod, container, true)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(logs)
}

func (c *Client) Logs(ctx context.Context, namespace, pod, container string, follow bool) (io.ReadCloser, error) {
	return c.client.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{
		Container: container,
		Follow:    follow,
	}).Stream(ctx)
}

type ExecStreamOptions struct {
	Stdin     io.Reader
	Stdout    io.Writer
	Stderr    io.Writer
	Pod       string
	Namespace string
	Container string
	Command   []string
	Log       loftlog.Logger
}

// Exec executes a kubectl exec with given transport round tripper and upgrader
func (c *Client) Exec(ctx context.Context, options *ExecStreamOptions) error {
	// HACK: suppress klog output from client-go unless debug mode is enabled.
	// When a WebSocket exec stream is closed (e.g. after inject completes), client-go's
	// heartbeat goroutine races with the connection teardown and logs spurious errors like
	// "Websocket Ping failed: use of closed network connection" via klog. This is a known
	// upstream issue in k8s.io/client-go/tools/remotecommand/websocket.go -- the heartbeat
	// doesn't respect context cancellation. Since DevPod uses its own logger (loft-sh/log),
	// silencing klog in non-debug mode is safe and avoids polluting the user's terminal.
	//
	// We use sync.Once here (not init()) because the --debug flag is only available after
	// cobra parses arguments in PersistentPreRunE, which runs after all init() functions.
	// By the time Exec is called, options.Log has the correct level set.
	setupKlogOnce.Do(func() {
		if options.Log == nil || options.Log.GetLevel() < logrus.DebugLevel {
			klog.SetLogger(logr.Discard())
		}
	})

	client, err := kubernetes.NewForConfig(c.config)
	if err != nil {
		return err
	}

	execRequest := client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(options.Pod).
		Namespace(options.Namespace).
		SubResource(string("exec")).
		VersionedParams(&corev1.PodExecOptions{
			Container: options.Container,
			Command:   options.Command,
			Stdin:     options.Stdin != nil,
			Stdout:    options.Stdout != nil,
			Stderr:    options.Stderr != nil,
		}, scheme.ParameterCodec)

	websocketExec, err := remotecommand.NewWebSocketExecutor(c.config, "POST", execRequest.URL().String())
	if err != nil {
		return err
	}
	spdyExec, err := remotecommand.NewSPDYExecutor(c.config, "POST", execRequest.URL())
	if err != nil {
		return err
	}
	exec, err := remotecommand.NewFallbackExecutor(websocketExec, spdyExec, func(err error) bool {
		if err != nil && err != context.Canceled {
			if options.Log != nil {
				options.Log.Warnf("WebSocket exec failed, falling back to SPDY: %v", err)
			}
			return true
		}
		return false
	})
	if err != nil {
		return err
	}

	if options.Log != nil {
		options.Log.Debugf("Exec [websocket+spdy-fallback]: pod=%s/%s container=%s cmd=%v",
			options.Namespace, options.Pod, options.Container, options.Command)
	}

	errChan := make(chan error)
	go func() {
		errChan <- exec.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdin:  options.Stdin,
			Stdout: options.Stdout,
			Stderr: options.Stderr,
		})
	}()

	select {
	case <-ctx.Done():
		<-errChan
		return nil
	case err = <-errChan:
		return err
	}
}
