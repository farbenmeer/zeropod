package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	goruntime "runtime"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/coreos/go-systemd/v22/dbus"
	v1 "github.com/ctrox/zeropod/api/shim/v1"
	"github.com/ctrox/zeropod/manager/node"
	"github.com/pelletier/go-toml/v2"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	criuImage              = flag.String("criu-image", "ghcr.io/ctrox/zeropod-criu:v4.2", "criu image to use.")
	runtime                = flag.String("runtime", "containerd", "specifies which runtime to configure. containerd/k3s/rke2")
	hostOptPath            = flag.String("host-opt-path", defaultOptPath, "path where zeropod binaries are stored on the host")
	uninstall              = flag.Bool("uninstall", false, "uninstalls zeropod by cleaning up all the files the installer created")
	installTimeout         = flag.Duration("timeout", time.Minute, "duration the installer waits for the installation to complete")
	versionFlag            = flag.Bool("version", false, "output version and exit")
	trackerIgnoreLocalhost = flag.Bool("tracker-ignore-localhost", v1.DefaultTrackerIgnoreLocalhost, "set to ignore traffic from localhost in socket tracker")
	capacityRequest        = flag.Bool("capacity-request", v1.DefaultCapacityRequest, "enable shim to make a capacity request before restoring")
	//lint:ignore U1000 kept for compatibility
	probeBinaryName = flag.String("probe-binary-name", v1.DefaultProbeBinaryName, "Deprecated: this is no longer used, flag will be removed in future release")

	version   = ""
	revision  = ""
	goVersion = goruntime.Version()
)

type containerRuntime string

const (
	runtimeContainerd containerRuntime = "containerd"
	runtimeRKE2       containerRuntime = "rke2"
	runtimeK3S        containerRuntime = "k3s"

	hostRoot                    = "/host"
	binPath                     = "bin/"
	criuConfigFile              = "/etc/criu/default.conf"
	shimBinaryName              = "containerd-shim-zeropod-v2"
	runtimePath                 = "/build/" + shimBinaryName
	defaultContainerdConfigPath = "/etc/containerd/config.toml"
	containerdSock              = "/run/containerd/containerd.sock"
	configBackupSuffix          = ".original"
	templateSuffix              = ".tmpl"
	caSecretName                = "ca-cert"
	criuConfig                  = `tcp-close
skip-in-flight
network-lock skip
`
	defaultOptPath    = "/opt/zeropod"
	containerdOptKey  = "io.containerd.internal.v1.opt"
	zeropodRuntimeKey = "containerd.runtimes.zeropod"
	optPlugin         = `
[plugins."io.containerd.internal.v1.opt"]
  path = "%s"
`
	zeropodTomlName = "runtime_zeropod.toml"
	runtimeConfigV3 = `version = 3

[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.zeropod]
  runtime_type = "io.containerd.runc.v2"
  runtime_path = "%s/bin/containerd-shim-zeropod-v2"
  pod_annotations = %s

  [plugins."io.containerd.cri.v1.runtime".containerd.runtimes.zeropod.options]
    # use systemd cgroup by default
    SystemdCgroup = true
`
	configVersion2 = "version = 2"
	runtimeConfig  = `
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.zeropod]
  runtime_type = "io.containerd.runc.v2"
  runtime_path = "%s/bin/containerd-shim-zeropod-v2"
  pod_annotations = %s

  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.zeropod.options]
    # use systemd cgroup by default
    SystemdCgroup = true
`
)

func init() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	if version == "" {
		version = info.Main.Version
	}

	for _, kv := range info.Settings {
		switch kv.Key {
		case "vcs.revision":
			revision = kv.Value
		}
	}
}

func main() {
	flag.Parse()

	if *versionFlag {
		printVersion()
		os.Exit(0)
	}
	log.Printf("starting installer version=%s revision=%s go=%s", version, revision, goVersion)

	client, err := inClusterClient()
	if err != nil {
		log.Fatalf("unable to create in-cluster client: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *installTimeout)
	defer cancel()

	if *uninstall {
		if err := runUninstall(ctx, client, containerRuntime(*runtime)); err != nil {
			log.Fatalf("error uninstalling zeropod: %s", err)
		}

		log.Println("uninstaller completed")
		os.Exit(0)
	}

	if err := installCriu(ctx); err != nil {
		log.Fatalf("error installing criu: %s", err)
	}

	log.Printf("installed criu binaries from %s", *criuImage)

	if err := installRuntime(ctx, containerRuntime(*runtime)); err != nil {
		log.Fatalf("error installing runtime: %s", err)
	}

	log.Println("installed runtime")

	if err := loadTLSCA(ctx, client); err != nil {
		log.Fatalf("error loading TLS CA certificate: %s", err)
	}

	log.Println("installed ca cert")

	log.Println("installer completed")
}

func printVersion() {
	fmt.Printf("%s:\n", filepath.Base(os.Args[0]))
	fmt.Println("  Version: ", version)
	fmt.Println("  Revision:", revision)
	fmt.Println("  Go version:", goVersion)
	fmt.Println("")
}

func installCriu(ctx context.Context) error {
	client, err := containerd.New(containerdSock, containerd.WithDefaultNamespace("k8s"))
	if err != nil {
		return err
	}

	image, err := client.Pull(ctx, *criuImage)
	if err != nil {
		return err
	}

	if err := client.Install(
		ctx, image, containerd.WithInstallLibs,
		containerd.WithInstallReplace,
		containerd.WithInstallPath(optPath(containerRuntime(*runtime))),
	); err != nil {
		return err
	}

	// write the criu config
	if err := os.MkdirAll(path.Dir(criuConfigFile), os.ModePerm); err != nil {
		return err
	}

	if err := os.WriteFile(criuConfigFile, []byte(criuConfig), 0644); err != nil {
		return err
	}

	return nil
}

func installRuntime(ctx context.Context, runtime containerRuntime) error {
	log.Printf("installing runtime for %s", runtime)

	conn, err := dbus.NewSystemdConnectionContext(ctx)
	if err != nil {
		return fmt.Errorf("unable to connect to dbus: %w", err)
	}

	opt := optPath(runtime)
	// note that if the shim binary already exists, we simply switch it out with
	// the new one but existing zeropods will have to be deleted to use the
	// updated shim.
	shimDest := filepath.Join(opt, binPath, shimBinaryName)
	if err := os.Remove(shimDest); err != nil {
		log.Printf("unable to remove shim binary, continuing with install: %s", err)
	}

	shim, err := os.ReadFile(runtimePath)
	if err != nil {
		return fmt.Errorf("unable to read shim file: %w", err)
	}

	if err := os.WriteFile(shimDest, shim, 0755); err != nil {
		return fmt.Errorf("unable to write shim file: %w", err)
	}

	b, err := json.MarshalIndent(&v1.Config{
		TrackerIgnoreLocalhost: *trackerIgnoreLocalhost,
		CapacityRequest:        *capacityRequest,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(opt, v1.ConfigDir), os.ModePerm); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(opt, v1.ConfigDir, v1.ConfigFileName), b, 0600); err != nil {
		return fmt.Errorf("unable to write shim file: %w", err)
	}

	if runtime == runtimeK3S {
		// for some reason, k3s containerd only has access to the busybox tar by
		// default. This breaks criu checkpoint since it needs the full gnu tar.
		// To work around this, we symlink tar in our opt path to /bin/tar.
		if err := linkTar(*hostOptPath); err != nil {
			return fmt.Errorf("unable to link tar: %w", err)
		}
	}

	restartRequired, err := configureContainerd(ctx, runtime)
	if err != nil {
		if restoreErr := restoreContainerdConfig(runtime, defaultContainerdConfigPath); restoreErr != nil {
			return fmt.Errorf("unable to configure and restore containerd config: %w: %w", restoreErr, err)
		}
		return fmt.Errorf("unable to configure containerd: %w", err)
	}

	if !restartRequired {
		return nil
	}

	switch runtime {
	case runtimeContainerd:
		return restartUnit(ctx, conn, "containerd.service")
	case runtimeRKE2:
		// for rke2/k3s we try restarting both services agent/server since we
		// don't know what our node is using. We return the error only if both
		// restarts fail.
		agentErr := restartUnit(ctx, conn, "rke2-agent.service")
		serverErr := restartUnit(ctx, conn, "rke2-server.service")

		if agentErr != nil && serverErr != nil {
			return fmt.Errorf("unable to restart rke2 agent/server: %w, %w", agentErr, serverErr)
		}

		return nil
	case runtimeK3S:
		agentErr := restartUnit(ctx, conn, "k3s-agent.service")
		serverErr := restartUnit(ctx, conn, "k3s.service")

		if agentErr != nil && serverErr != nil {
			return fmt.Errorf("unable to restart k3s agent/server: %w, %w", agentErr, serverErr)
		}

		return nil
	}

	return nil
}

func restartUnit(ctx context.Context, conn *dbus.Conn, service string) error {
	ch := make(chan string)
	if _, err := conn.TryRestartUnitContext(ctx, service, "replace", ch); err != nil {
		return fmt.Errorf("unable to restart %s", service)
	}
	<-ch

	return nil
}

func configureContainerd(ctx context.Context, runtime containerRuntime) (restartRequired bool, err error) {
	client, err := containerd.New(containerdSock, containerd.WithDefaultNamespace("k8s"))
	if err != nil {
		return false, fmt.Errorf("creating containerd client: %w", err)
	}

	v, err := client.Version(ctx)
	if err != nil {
		return false, fmt.Errorf("getting containerd version: %w", err)
	}
	log.Printf("configuring containerd %s", v.Version)
	if strings.HasPrefix(v.Version, "1") || strings.HasPrefix(v.Version, "v1") {
		return configureContainerdv1(ctx, runtime, defaultContainerdConfigPath)
	}
	return configureContainerdv2(ctx, runtime, defaultContainerdConfigPath)
}

// containerdConfig is the subset of containerd's configuration that the
// installer needs: the config version, the imports and the opt plugin path.
//
// It is parsed directly instead of going through containerd's own config loader
// because that loader rejects any config with a version higher than the
// containerd release the installer was built against — containerd/v2 v2.1 caps
// at version 3, while containerd v2.3 writes version 4. The installer would then
// refuse to run on a host whose containerd is newer than the one it vendors,
// even though the runtime drop-in it writes is perfectly valid there (drop-ins
// may declare a lower version than the root config and are migrated forward).
// Parsing only the few fields we care about keeps the installer working against
// config versions that did not exist when it was built.
type containerdConfig struct {
	Version int                       `toml:"version"`
	Imports []string                  `toml:"imports"`
	Plugins map[string]map[string]any `toml:"plugins"`
}

// loadContainerdConfig parses the containerd config at path along with its
// imports. Imported files only add to the root config, they never override it,
// which is all the installer needs to detect an existing zeropod import or a
// pre-configured opt plugin.
func loadContainerdConfig(path string) (*containerdConfig, error) {
	conf := &containerdConfig{}
	if err := decodeContainerdConfig(path, conf); err != nil {
		return nil, err
	}

	for _, imp := range resolveImports(path, conf.Imports) {
		dropIn := &containerdConfig{}
		if err := decodeContainerdConfig(imp, dropIn); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// an import that does not (yet) exist is not fatal, containerd
				// itself also tolerates it.
				continue
			}
			return nil, err
		}
		if dropIn.Version > conf.Version {
			return nil, fmt.Errorf(
				"drop-in config %s version %d is higher than root config version %d",
				imp, dropIn.Version, conf.Version,
			)
		}
		for name, plugin := range dropIn.Plugins {
			if _, ok := conf.Plugins[name]; ok {
				continue
			}
			if conf.Plugins == nil {
				conf.Plugins = map[string]map[string]any{}
			}
			conf.Plugins[name] = plugin
		}
	}

	return conf, nil
}

func decodeContainerdConfig(path string, out *containerdConfig) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := toml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("failed to load TOML from %s: %w", path, err)
	}
	return nil
}

// resolveImports resolves import paths relative to the config that declares
// them, the same way containerd does.
func resolveImports(parent string, imports []string) []string {
	var out []string
	for _, path := range imports {
		path = filepath.Clean(path)
		if !filepath.IsAbs(path) {
			path = filepath.Join(filepath.Dir(parent), path)
		}
		if strings.Contains(path, "*") {
			matches, err := filepath.Glob(path)
			if err != nil {
				// the only possible error is a malformed pattern, which we
				// simply skip over as containerd would fail on it anyway.
				continue
			}
			out = append(out, matches...)
			continue
		}
		out = append(out, path)
	}
	return out
}

// optPluginPath returns the path configured for containerd's opt plugin, or an
// empty string if it is not configured.
func (c *containerdConfig) optPluginPath() string {
	path, _ := c.Plugins[containerdOptKey]["path"].(string)
	return path
}

func configureContainerdv2(ctx context.Context, runtime containerRuntime, containerdConfig string) (bool, error) {
	if err := migrateToImports(runtime, containerdConfig); err != nil {
		return false, fmt.Errorf("migrating to imports: %w", err)
	}

	conf, err := loadContainerdConfig(containerdConfig)
	if err != nil {
		return false, fmt.Errorf("loading containerd config: %w", err)
	}

	if zeropodImportConfigured(conf.Imports) {
		log.Println("runtime already configured, no need to restart containerd")
		return false, nil
	}

	containerdOptPath := conf.optPluginPath()
	existingOpt := containerdOptPath != ""

	if err := backupContainerdConfig(containerdConfig); err != nil {
		return false, fmt.Errorf("backing up containerd config: %w", err)
	}

	if runtime == runtimeRKE2 || runtime == runtimeK3S {
		// for rke2/k3s the containerd config has to be customized via the
		// config.toml.tmpl file. So we make a copy of the original config and
		// insert our shim config into the template.
		if err := copyConfig(containerdConfig, containerdConfig+templateSuffix); err != nil {
			return false, fmt.Errorf("unable to copy config template: %w", err)
		}
		containerdConfig = containerdConfig + templateSuffix
	}

	if err := addZeropodConfigImport(containerdConfig, conf); err != nil {
		return false, err
	}

	optPath := *hostOptPath
	if existingOpt {
		optPath = containerdOptPath
	}

	if err := writeZeropodRuntimeConfig(containerdConfig, optPath, existingOpt, conf.Version); err != nil {
		return false, err
	}

	// sanity check config by loading it again
	if _, err := loadContainerdConfig(containerdConfig); err != nil {
		return false, fmt.Errorf("loading modified containerd config: %w", err)
	}

	return true, nil
}

func configureContainerdv1(ctx context.Context, runtime containerRuntime, containerdConfig string) (bool, error) {
	confContents, err := os.ReadFile(containerdConfig)
	if err != nil {
		return false, err
	}
	if strings.Contains(string(confContents), zeropodRuntimeKey) {
		log.Println("runtime already configured, no need to restart containerd")
		return false, nil
	}

	// backup the original config
	if err := copyConfig(containerdConfig, containerdConfig+configBackupSuffix); err != nil {
		return false, err
	}

	if runtime == runtimeRKE2 || runtime == runtimeK3S {
		// for rke2/k3s the containerd config has to be customized via the
		// config.toml.tmpl file. So we make a copy of the original config and
		// insert our shim config into the template.
		if err := copyConfig(containerdConfig, containerdConfig+templateSuffix); err != nil {
			return false, fmt.Errorf("unable to copy config template: %w", err)
		}
		containerdConfig = containerdConfig + templateSuffix
	}

	cfg, err := os.OpenFile(containerdConfig, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return false, err
	}

	configured, containerdOptPath, err := optConfigured(containerdConfig)
	if err != nil {
		return false, err
	}

	optPath := *hostOptPath
	if configured {
		optPath = containerdOptPath
	}

	if _, err := fmt.Fprintf(
		cfg, runtimeConfig,
		strings.TrimSuffix(optPath, "/"),
		annotationsToml(),
	); err != nil {
		return false, err
	}

	if !configured {
		if _, err := fmt.Fprintf(cfg, optPlugin, *hostOptPath); err != nil {
			return false, err
		}
	}

	return true, nil
}

func addZeropodConfigImport(containerdConfigPath string, conf *containerdConfig) error {
	importsConf := struct {
		Imports []string `toml:"imports"`
	}{}
	importsConf.Imports = conf.Imports
	importsConf.Imports = append(importsConf.Imports, zeropodTomlName)
	imports, err := toml.Marshal(importsConf)
	if err != nil {
		return err
	}

	cfgData, err := os.ReadFile(containerdConfigPath)
	if err != nil {
		return fmt.Errorf("opening containerd config: %w", err)
	}
	lines := strings.Split(string(cfgData), "\n")

	start, end, found := findImportsLines(lines)
	if found {
		if start == end {
			end++
		}
		lines = slices.Delete(lines, start, end)
	}

	vLine, ok := versionLine(lines)
	if !ok {
		return fmt.Errorf("version not found in containerd config")
	}
	lines = slices.Insert(lines, vLine+1, strings.TrimSpace(string(imports)))

	if err := os.WriteFile(containerdConfigPath, []byte(strings.Join(lines, "\n")), 0644); err != nil {
		return fmt.Errorf("writing containerd config: %w", err)
	}
	return nil
}

func versionLine(lines []string) (pos int, found bool) {
	for i, line := range lines {
		if strings.Contains(strings.TrimSpace(line), "version") {
			return i, true
		}
	}
	return 0, false
}

func findImportsLines(lines []string) (start int, end int, found bool) {
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "imports") {
			// handle multiline array
			if !strings.HasSuffix(strings.TrimSpace(line), "]") {
				for j, line := range lines[i:] {
					if strings.HasSuffix(strings.TrimSpace(line), "]") {
						return i, i + j + 1, true
					}
				}
			}
			return i, i, true
		}
	}
	return 0, 0, false
}

func zeropodImportConfigured(imports []string) bool {
	for _, imp := range imports {
		if filepath.Base(imp) == zeropodTomlName {
			return true
		}
	}
	return false
}

func zeropodRuntimeConfigPath(containerdConfig string) string {
	return filepath.Join(filepath.Dir(containerdConfig), zeropodTomlName)
}

func backupContainerdConfig(containerdConfig string) error {
	return copyConfig(containerdConfig, containerdConfig+configBackupSuffix)
}

func writeZeropodRuntimeConfig(containerdConfig, optPath string, existingOpt bool, version int) error {
	zeropodRuntimeConfig := fmt.Sprintf("%s\n%s", configVersion2, runtimeConfig)
	if version >= 3 {
		// the CRI plugin was split into io.containerd.cri.v1.runtime/images in
		// config version 3 and kept those names since. A drop-in may declare a
		// lower version than the root config, so writing the version 3 form is
		// also correct for any later version.
		zeropodRuntimeConfig = runtimeConfigV3
	}

	zeropodRuntimeConfig = fmt.Sprintf(
		zeropodRuntimeConfig,
		strings.TrimSuffix(optPath, "/"),
		annotationsToml(),
	)
	if !existingOpt {
		zeropodRuntimeConfig = zeropodRuntimeConfig + fmt.Sprintf(optPlugin, optPath)
	}
	if err := os.WriteFile(zeropodRuntimeConfigPath(containerdConfig), []byte(zeropodRuntimeConfig), 0644); err != nil {
		return fmt.Errorf("writing zeropod runtime config: %w", err)
	}
	return nil
}

func annotationsToml() string {
	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	enc.SetArraysMultiline(true)
	enc.SetIndentSymbol("    ")
	if err := enc.Encode(v1.ContainerdAnnotations); err != nil {
		return "[]"
	}
	return buf.String()
}

func restoreContainerdConfig(runtime containerRuntime, containerdConfigPath string) error {
	if _, err := os.Stat(containerdConfigPath + configBackupSuffix); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Println("could not find config backup, either it has already been restored or it never existed")
			return nil
		}
	}

	if err := copyConfig(containerdConfigPath+configBackupSuffix, containerdConfigFile(runtime, containerdConfigPath)); err != nil {
		return err
	}

	if err := os.Remove(containerdConfigPath + configBackupSuffix); err != nil {
		return err
	}

	return nil
}

func migrateToImports(runtime containerRuntime, containerdConfigPath string) error {
	cfg, err := os.ReadFile(containerdConfigFile(runtime, containerdConfigPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("unable to read containerd config: %w", err)
	}
	if strings.Contains(string(cfg), "containerd.runtimes.zeropod") {
		if err := restoreContainerdConfig(runtime, containerdConfigPath); err != nil {
			return fmt.Errorf("unable to restore original config: %w", err)
		}
	}
	return nil
}

func containerdConfigFile(runtime containerRuntime, containerdConfigPath string) string {
	if strings.HasSuffix(containerdConfigPath, templateSuffix) {
		return containerdConfigPath
	}

	if runtime == runtimeRKE2 || runtime == runtimeK3S {
		containerdConfigPath = containerdConfigPath + templateSuffix
	}

	return containerdConfigPath
}

func copyConfig(from, to string) error {
	info, err := os.Stat(from)
	if err != nil {
		return fmt.Errorf("could not stat containerd config: %w", err)
	}
	originalConfig, err := os.ReadFile(from)
	if err != nil {
		return fmt.Errorf("could not read containerd config: %w", err)
	}
	if err := os.WriteFile(to, originalConfig, info.Mode()); err != nil {
		return fmt.Errorf("could not write config backup: %w", err)
	}

	return nil
}

func optConfigured(containerdConfig string) (bool, string, error) {
	conf, err := loadContainerdConfig(containerdConfig)
	if err != nil {
		return false, "", err
	}

	if path := conf.optPluginPath(); path != "" {
		return true, path, nil
	}

	return false, "", nil
}

func optPath(runtime containerRuntime) string {
	ok, path, err := optConfigured(containerdConfigFile(runtime, defaultContainerdConfigPath))
	if err != nil {
		return defaultOptPath
	}
	if ok {
		return filepath.Join(hostRoot, path)
	}
	return defaultOptPath
}

func inClusterClient() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}

// runUninstall removes all components installed by zeropod and restores the
// original configuration.
func runUninstall(ctx context.Context, client kubernetes.Interface, runtime containerRuntime) error {
	if err := os.RemoveAll(optPath(runtime)); err != nil {
		return fmt.Errorf("removing opt path: %w", err)
	}

	if err := restoreContainerdConfig(runtime, defaultContainerdConfigPath); err != nil {
		return err
	}

	return nil
}

func loadTLSCA(ctx context.Context, client kubernetes.Interface) error {
	// TODO: do not hardcode
	namespace := "zeropod-system"
	secret, err := client.CoreV1().Secrets(namespace).Get(ctx, caSecretName, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			secret, err = generateTLSCA(ctx, client, namespace)
			if err != nil {
				return err
			}
		}
	}
	if err := os.WriteFile("/tls/ca.crt", secret.Data[corev1.TLSCertKey], 0600); err != nil {
		return err
	}
	if err := os.WriteFile("/tls/ca.key", secret.Data[corev1.TLSPrivateKeyKey], 0600); err != nil {
		return err
	}

	return nil
}

func generateTLSCA(ctx context.Context, client kubernetes.Interface, namespace string) (*corev1.Secret, error) {
	ca, err := node.GenCert(nil, nil)
	if err != nil {
		return nil, err
	}

	certOut := new(bytes.Buffer)
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: ca.Certificate[0]}); err != nil {
		return nil, fmt.Errorf("failed to write data to cert: %w", err)
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(ca.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal private key: %w", err)
	}

	keyOut := new(bytes.Buffer)
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		return nil, fmt.Errorf("failed to write data to key: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      caSecretName,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			corev1.TLSCertKey:       certOut.Bytes(),
			corev1.TLSPrivateKeyKey: keyOut.Bytes(),
		},
		Type: corev1.SecretTypeTLS,
	}
	secret, err = client.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	if kerrors.IsAlreadyExists(err) {
		return client.CoreV1().Secrets(namespace).Get(ctx, caSecretName, metav1.GetOptions{})
	}

	return secret, err
}

func linkTar(opt string) error {
	if err := os.Symlink("/bin/tar", filepath.Join(opt, "bin", "tar")); !os.IsExist(err) {
		return err
	}
	return nil
}
