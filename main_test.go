package main_test

import (
	"io"
	"net/http"
	"os/exec"
	"strconv"

	"encoding/json"
	"io/ioutil"

	"os"

	"github.com/pivotal-cf/brokerapi"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("csibroker Main", func() {
	var (
		args               []string
		listenAddr         string
		tempDir            string
		username, password string

		process ifrit.Process
	)

	BeforeEach(func() {
		listenAddr = "0.0.0.0:" + strconv.Itoa(8999+GinkgoParallelNode())
		username = "admin"
		password = "password"
		tempDir = os.TempDir()

		os.Setenv("USERNAME", username)
		os.Setenv("PASSWORD", password)

		args = append(args, "-listenAddr", listenAddr)
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
			args = append(args, "-serviceName", "something")
			args = append(args, "-serviceId", "someguid")
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

			Expect(catalog.Services[0].Name).To(Equal("something"))
			Expect(catalog.Services[0].ID).To(Equal("someguid"))
			Expect(catalog.Services[0].Plans[0].ID).To(Equal("Existing"))
			Expect(catalog.Services[0].Plans[0].Name).To(Equal("Existing"))
			Expect(catalog.Services[0].Plans[0].Description).To(Equal("A preexisting filesystem"))
		})
	})
})
