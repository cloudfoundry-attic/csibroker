package main_test

import (
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"

	"encoding/json"
	"io/ioutil"

	"os"

	"github.com/pivotal-cf/brokerapi"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

type failRunner struct {
	Command           *exec.Cmd
	Name              string
	AnsiColorCode     string
	StartCheck        string
	StartCheckTimeout time.Duration
	Cleanup           func()
	session           *gexec.Session
	sessionReady      chan struct{}
}

func (r failRunner) Run(sigChan <-chan os.Signal, ready chan<- struct{}) error {
	defer GinkgoRecover()

	allOutput := gbytes.NewBuffer()

	debugWriter := gexec.NewPrefixedWriter(
		fmt.Sprintf("\x1b[32m[d]\x1b[%s[%s]\x1b[0m ", r.AnsiColorCode, r.Name),
		GinkgoWriter,
	)

	session, err := gexec.Start(
		r.Command,
		gexec.NewPrefixedWriter(
			fmt.Sprintf("\x1b[32m[o]\x1b[%s[%s]\x1b[0m ", r.AnsiColorCode, r.Name),
			io.MultiWriter(allOutput, GinkgoWriter),
		),
		gexec.NewPrefixedWriter(
			fmt.Sprintf("\x1b[91m[e]\x1b[%s[%s]\x1b[0m ", r.AnsiColorCode, r.Name),
			io.MultiWriter(allOutput, GinkgoWriter),
		),
	)

	Ω(err).ShouldNot(HaveOccurred())

	fmt.Fprintf(debugWriter, "spawned %s (pid: %d)\n", r.Command.Path, r.Command.Process.Pid)

	r.session = session
	if r.sessionReady != nil {
		close(r.sessionReady)
	}

	startCheckDuration := r.StartCheckTimeout
	if startCheckDuration == 0 {
		startCheckDuration = 5 * time.Second
	}

	var startCheckTimeout <-chan time.Time
	if r.StartCheck != "" {
		startCheckTimeout = time.After(startCheckDuration)
	}

	detectStartCheck := allOutput.Detect(r.StartCheck)

	for {
		select {
		case <-detectStartCheck: // works even with empty string
			allOutput.CancelDetects()
			startCheckTimeout = nil
			detectStartCheck = nil
			close(ready)

		case <-startCheckTimeout:
			// clean up hanging process
			session.Kill().Wait()

			// fail to start
			return fmt.Errorf(
				"did not see %s in command's output within %s. full output:\n\n%s",
				r.StartCheck,
				startCheckDuration,
				string(allOutput.Contents()),
			)

		case signal := <-sigChan:
			session.Signal(signal)

		case <-session.Exited:
			if r.Cleanup != nil {
				r.Cleanup()
			}

			Expect(string(allOutput.Contents())).To(ContainSubstring(r.StartCheck))
			Expect(session.ExitCode()).To(Not(Equal(0)), fmt.Sprintf("Expected process to exit with non-zero, got: 0"))
			return nil
		}
	}
}

var _ = Describe("csibroker Main", func() {
	var (
		csiConAddr   string
		tempDir      string
		pwd          string
		err          error
		specFilepath string
	)
	BeforeEach(func() {
		tempDir = os.TempDir()
		pwd, err = os.Getwd()
		Expect(err).ToNot(HaveOccurred())
		csiConAddr = "0.0.0.0:" + strconv.Itoa(5005+GinkgoParallelNode())
		specFilepath = filepath.Join(pwd, "fixtures", "service_spec.json")
	})

	Context("Missing required args", func() {
		var process ifrit.Process
		It("shows usage to include dataDir", func() {
			var args []string
			volmanRunner := failRunner{
				Name:       "nfsbroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "dataDir must be provided.",
			}
			process = ifrit.Invoke(volmanRunner)

		})

		It("shows usage to include csiConAddr", func() {
			var args []string
			args = append(args, "-dataDir", tempDir)
			volmanRunner := failRunner{
				Name:       "nfsbroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "csiConAddr must be provided.",
			}
			process = ifrit.Invoke(volmanRunner)

		})

		It("shows usage to include serviceSpec", func() {
			var args []string
			args = append(args, "-dataDir", tempDir)
			args = append(args, "-csiConAddr", csiConAddr)
			volmanRunner := failRunner{
				Name:       "nfsbroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "serviceSpec must be provided.",
			}
			process = ifrit.Invoke(volmanRunner)

		})

		AfterEach(func() {
			ginkgomon.Kill(process) // this is only if incorrect implementation leaves process running
		})
	})

	Context("Has required args", func() {

		var (
			args               []string
			listenAddr         string
			csiConAddr         string
			username, password string

			process ifrit.Process
		)

		BeforeEach(func() {
			listenAddr = "0.0.0.0:" + strconv.Itoa(8999+GinkgoParallelNode())
			username = "admin"
			password = "password"
			csiConAddr = "0.0.0.0:" + strconv.Itoa(5005+GinkgoParallelNode())

			args = append(args, "-listenAddr", listenAddr)
			args = append(args, "-dataDir", tempDir)
			args = append(args, "-username", username)
			args = append(args, "-password", password)
			args = append(args, "-csiConAddr", csiConAddr)
			args = append(args, "-serviceSpec", specFilepath)
		})

		JustBeforeEach(func() {
			volmanRunner := ginkgomon.New(ginkgomon.Config{
				Name:       "csibroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "started",
			})
			process = ginkgomon.Invoke(volmanRunner)
		})

		AfterEach(func() {
			ginkgomon.Kill(process)
		})

		httpDoWithAuth := func(method, endpoint string, body io.ReadCloser) (*http.Response, error) {
			req, err := http.NewRequest(method, "http://"+listenAddr+endpoint, body)
			Expect(err).NotTo(HaveOccurred())

			req.SetBasicAuth(username, password)
			return http.DefaultClient.Do(req)
		}

		It("should listen on the given address", func() {
			resp, err := httpDoWithAuth("GET", "/v2/catalog", nil)
			Expect(err).NotTo(HaveOccurred())

			Expect(resp.StatusCode).To(Equal(200))
		})

		Context("given arguments", func() {
			BeforeEach(func() {
				args = append(args, "-serviceSpec", specFilepath)
			})

			It("should pass arguments though to catalog", func() {
				resp, err := httpDoWithAuth("GET", "/v2/catalog", nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(200))

				bytes, err := ioutil.ReadAll(resp.Body)
				Expect(err).NotTo(HaveOccurred())

				var catalog brokerapi.CatalogResponse
				err = json.Unmarshal(bytes, &catalog)
				Expect(err).NotTo(HaveOccurred())

				Expect(catalog.Services[0].Name).To(Equal("Service.Name"))
				Expect(catalog.Services[0].ID).To(Equal("Service.ID"))
				Expect(catalog.Services[0].Plans[0].ID).To(Equal("Service.Plans.ID"))
				Expect(catalog.Services[0].Plans[0].Name).To(Equal("Service.Plans.Name"))
				Expect(catalog.Services[0].Plans[0].Description).To(Equal("Service.Plans.Description"))
			})
		})
	})
})
