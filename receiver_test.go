// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheuscollectorbridge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestEndToEnd_FullReceiverPipeline(t *testing.T) {
	shutdownCalled := false
	var startID, shutdownID component.ID
	var shutdownSettings receiver.Settings
	lifecycleManager := &mockLifecycleManager{
		startFunc: func(_ context.Context, set receiver.Settings, _ any) (*prometheus.Registry, component.ShutdownFunc, error) {
			startID = set.ID
			shutdownSettings = set
			reg := prometheus.NewRegistry()

			counter := prometheus.NewCounter(prometheus.CounterOpts{
				Name: "myapp_requests_total",
				Help: "Total requests handled",
			})
			gauge := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "myapp_connections_active",
				Help: "Current active connections",
			})

			reg.MustRegister(counter, gauge)
			counter.Add(1500)
			gauge.Set(42)

			return reg, func(_ context.Context) error {
				shutdownID = shutdownSettings.ID
				shutdownCalled = true
				return nil
			}, nil
		},
	}

	receiverType := component.MustNewType("myapp")
	factory := NewFactory(
		receiverType,
		lifecycleManager,
		&mockConfigUnmarshaler{},
	)

	cfg := &ReceiverConfig{
		ScrapeInterval: 100 * time.Millisecond,
	}

	sink := new(consumertest.MetricsSink)
	set := receivertest.NewNopSettings(receiverType)

	receiver, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	if err != nil {
		t.Fatalf("CreateMetrics() failed: %v", err)
	}

	err = receiver.Start(context.Background(), componenttest.NewNopHost())
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for sink.DataPointCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for metrics to arrive in sink")
		case <-time.After(10 * time.Millisecond):
		}
	}

	allMetrics := sink.AllMetrics()
	if len(allMetrics) == 0 {
		t.Fatal("expected at least one batch of metrics in sink")
	}

	foundMetrics := collectMetricNames(allMetrics[0])
	if _, found := foundMetrics["myapp_requests_total"]; !found {
		t.Error("metric myapp_requests_total not found in sink output")
	}
	if _, found := foundMetrics["myapp_connections_active"]; !found {
		t.Error("metric myapp_connections_active not found in sink output")
	}

	err = receiver.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown() failed: %v", err)
	}
	if !shutdownCalled {
		t.Error("lifecycle manager Shutdown() was not called")
	}
	if startID != set.ID {
		t.Errorf("Start() settings ID = %q, want %q", startID, set.ID)
	}
	if shutdownID != set.ID {
		t.Errorf("Shutdown() settings ID = %q, want %q", shutdownID, set.ID)
	}
}

func TestEndToEnd_MultipleScrapes(t *testing.T) {
	lifecycleManager := &mockLifecycleManager{
		startFunc: func(_ context.Context, _ receiver.Settings, _ any) (*prometheus.Registry, component.ShutdownFunc, error) {
			reg := prometheus.NewRegistry()
			gauge := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "scrape_test_metric",
				Help: "test",
			})
			reg.MustRegister(gauge)
			gauge.Set(1)
			return reg, nil, nil
		},
	}

	receiverType := component.MustNewType("scrapetest")
	factory := NewFactory(receiverType, lifecycleManager, &mockConfigUnmarshaler{})

	cfg := &ReceiverConfig{
		ScrapeInterval: 50 * time.Millisecond,
	}

	sink := new(consumertest.MetricsSink)
	set := receivertest.NewNopSettings(receiverType)

	receiver, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	if err != nil {
		t.Fatalf("CreateMetrics() failed: %v", err)
	}

	err = receiver.Start(context.Background(), componenttest.NewNopHost())
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for len(sink.AllMetrics()) < 3 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for 3 scrapes, got %d", len(sink.AllMetrics()))
		case <-time.After(10 * time.Millisecond):
		}
	}

	scrapeCount := len(sink.AllMetrics())
	if scrapeCount < 3 {
		t.Errorf("expected at least 3 scrape batches, got %d", scrapeCount)
	}

	err = receiver.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown() failed: %v", err)
	}
}

func TestEndToEnd_ReceiverInstancesOwnTheirShutdown(t *testing.T) {
	shutdownCalls := map[component.ID]int{}
	lifecycleManager := &mockLifecycleManager{
		startFunc: func(_ context.Context, set receiver.Settings, _ any) (*prometheus.Registry, component.ShutdownFunc, error) {
			id := set.ID
			return prometheus.NewRegistry(), func(_ context.Context) error {
				shutdownCalls[id]++
				return nil
			}, nil
		},
	}

	receiverType := component.MustNewType("multiinstance")
	factory := NewFactory(receiverType, lifecycleManager, &mockConfigUnmarshaler{})
	consumer := new(consumertest.MetricsSink)

	firstSettings := receivertest.NewNopSettings(receiverType)
	firstSettings.ID = component.NewIDWithName(receiverType, "first")
	first, err := factory.CreateMetrics(
		context.Background(),
		firstSettings,
		&ReceiverConfig{ScrapeInterval: time.Hour},
		consumer,
	)
	if err != nil {
		t.Fatalf("CreateMetrics() for first receiver failed: %v", err)
	}

	secondSettings := receivertest.NewNopSettings(receiverType)
	secondSettings.ID = component.NewIDWithName(receiverType, "second")
	second, err := factory.CreateMetrics(
		context.Background(),
		secondSettings,
		&ReceiverConfig{ScrapeInterval: time.Hour},
		consumer,
	)
	if err != nil {
		t.Fatalf("CreateMetrics() for second receiver failed: %v", err)
	}

	host := componenttest.NewNopHost()
	if err := first.Start(context.Background(), host); err != nil {
		t.Fatalf("Start() for first receiver failed: %v", err)
	}
	if err := second.Start(context.Background(), host); err != nil {
		t.Fatalf("Start() for second receiver failed: %v", err)
	}

	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() for first receiver failed: %v", err)
	}
	if got := shutdownCalls[firstSettings.ID]; got != 1 {
		t.Errorf("first receiver shutdown calls = %d, want 1", got)
	}
	if got := shutdownCalls[secondSettings.ID]; got != 0 {
		t.Errorf("second receiver shutdown calls = %d before its shutdown, want 0", got)
	}

	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() for second receiver failed: %v", err)
	}
	if got := shutdownCalls[secondSettings.ID]; got != 1 {
		t.Errorf("second receiver shutdown calls = %d, want 1", got)
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeated Shutdown() failed: %v", err)
	}
	if got := shutdownCalls[firstSettings.ID]; got != 1 {
		t.Errorf("first receiver shutdown calls = %d after repeated shutdown, want 1", got)
	}
}

func TestReceiverShutdownBeforeStart(t *testing.T) {
	r := newPrometheusReceiver(&ReceiverConfig{}, new(consumertest.MetricsSink),
		receivertest.NewNopSettings(component.MustNewType("test")), &mockLifecycleManager{})
	for range 2 {
		if err := r.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() failed: %v", err)
		}
	}
}

type shutdownTestController struct {
	component.StartFunc
	component.ShutdownFunc
}

type failingMeterProvider struct {
	metric.MeterProvider
	err error
}

func (p failingMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return failingMeter{Meter: noop.NewMeterProvider().Meter("test"), err: p.err}
}

type failingMeter struct {
	metric.Meter
	err error
}

func (m failingMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, m.err
}

func TestReceiverShutdownAfterControllerCreationFailure(t *testing.T) {
	setupErr := errors.New("telemetry setup failed")
	set := receivertest.NewNopSettings(component.MustNewType("test"))
	set.MeterProvider = failingMeterProvider{MeterProvider: noop.NewMeterProvider(), err: setupErr}
	shutdownCalls := 0
	r := newPrometheusReceiver(&ReceiverConfig{ScrapeInterval: time.Hour}, new(consumertest.MetricsSink), set,
		&mockLifecycleManager{startFunc: func(context.Context, receiver.Settings, any) (*prometheus.Registry, component.ShutdownFunc, error) {
			return prometheus.NewRegistry(), func(context.Context) error {
				shutdownCalls++
				return nil
			}, nil
		}})
	if err := r.Start(context.Background(), componenttest.NewNopHost()); !errors.Is(err, setupErr) {
		t.Fatalf("Start() error = %v, want %v", err, setupErr)
	}
	if shutdownCalls != 0 {
		t.Fatal("Start() cleaned up exporter before the Collector called Shutdown()")
	}
	for range 2 {
		if err := r.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() failed: %v", err)
		}
	}
	if shutdownCalls != 1 {
		t.Fatalf("exporter shutdown calls = %d, want 1", shutdownCalls)
	}
}

func TestReceiverShutdownClearsHandlesEvenOnError(t *testing.T) {
	controllerErr := errors.New("controller shutdown failed")
	exporterErr := errors.New("exporter shutdown failed")
	var controllerCalls, exporterCalls int
	r := &prometheusReceiver{
		settings: receivertest.NewNopSettings(component.MustNewType("test")),
		controller: &shutdownTestController{ShutdownFunc: func(context.Context) error {
			controllerCalls++
			return controllerErr
		}},
		shutdownExporter: func(context.Context) error {
			exporterCalls++
			return exporterErr
		},
	}
	err := r.Shutdown(context.Background())
	if !errors.Is(err, controllerErr) || !errors.Is(err, exporterErr) {
		t.Fatalf("Shutdown() error = %v, want both cleanup errors", err)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeated Shutdown() failed: %v", err)
	}
	if controllerCalls != 1 || exporterCalls != 1 {
		t.Fatalf("cleanup calls = (%d, %d), want (1, 1)", controllerCalls, exporterCalls)
	}
}
