package fas

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync/atomic"
	"time"
	"github.com/expr-lang/expr"
	"github.com/superfly/fly-go"
)

// Reconciler represents the central part of the autoscaler that stores metrics,
// computes the number of necessary machines, and performs scaling.
type Reconciler struct {
	metrics   map[string]float64
	regionSeq atomic.Int64

	// Client to connect to Machines API to scale app. Required.
	Client FlapsClient

	// The name of the app currently being reconciled.
	AppName string

	// List of regions that machines can be created in.
	// The reconciler uses a round-robin approach to choosing next region.
	Regions []string

	// Expression used for calculating the number of created machines.
	// If current number is less than min, more machines will be created.
	// If current number is more than max, machines will be destroyed.
	MinCreatedMachineN string
	MaxCreatedMachineN string

	// Expression used for calculating the number of currently started machines.
	// If current number is less than min, more machines will be started.
	// If current number is more than max, machines will be stopped.
	MinStartedMachineN string
	MaxStartedMachineN string

	// Initial machine state (started or stopped)
	InitialMachineState string

	// List of collectors to fetch metric values from.
	Collectors []MetricCollector

	// Must also be registered in RegisterPromMetrics() for visibility.
	Stats *ReconcilerStats

	// Scale step size.
	ScaleUpStepSize int
	ScaleDownStepSize int

	// Scale step interval.
	ScaleUpStepInterval time.Duration
	ScaleDownStepInterval time.Duration

	// Last time we scaled up.
	lastScaleUpTime time.Time

	// Last time we scaled down.
	lastScaleDownTime time.Time
}

func NewReconciler() *Reconciler {
	return &Reconciler{
		metrics: make(map[string]float64),
		Stats:   &ReconcilerStats{},
	}
}

// NextRegion returns the next region to launch a machine in.
// If Regions is empty, returns a blank string.
func (r *Reconciler) NextRegion() string {
	if len(r.Regions) == 0 {
		return ""
	}

	i := int(r.regionSeq.Add(1))
	return r.Regions[(i-1)%len(r.Regions)]
}

// Value returns the value of a named metric and whether the metric has been set.
func (r *Reconciler) Value(name string) (float64, bool) {
	v, ok := r.metrics[name]
	return v, ok
}

// SetValue sets the value of a named metric.
func (r *Reconciler) SetValue(name string, value float64) {
	r.metrics[name] = value
}

// CollectMetrics fetches metrics from all collectors.
func (r *Reconciler) CollectMetrics(ctx context.Context) error {
	// Clear all metrics before each collection as the reconciler can be shared.
	r.metrics = make(map[string]float64)

	for _, c := range r.Collectors {
		value, err := c.CollectMetric(ctx, r.AppName)
		if err != nil {
			return fmt.Errorf("collect metric (%q): %w", c.Name(), err)
		}
		r.SetValue(c.Name(), value)
	}
	return nil
}

func reachbleMachines(machines []*fly.Machine) []*fly.Machine {
	var reachable []*fly.Machine
	for _, m := range machines {
		if m.HostStatus == fly.HostStatusOk {
			reachable = append(reachable, m)
		}
	}
	return reachable
}

// Reconcile scales the number of machines up, if needed. Machines should shut
// themselves down to scale down. Returns the number of started machines, if any.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	// Compute number of machines based on expr & metrics
	minCreatedN, hasMinCreatedN, err := r.CalcMinCreatedMachineN()
	if err != nil {
		return fmt.Errorf("compute minimum created machine count: %w", err)
	}
	maxCreatedN, hasMaxCreatedN, err := r.CalcMaxCreatedMachineN()
	if err != nil {
		return fmt.Errorf("compute minimum created machine count: %w", err)
	}

	minStartedN, hasMinStartedN, err := r.CalcMinStartedMachineN()
	slog.Info("minStartedN machine details", slog.Int("minStartedN", minStartedN), slog.Bool("hasMinStartedN", hasMinStartedN))
	if err != nil {
		return fmt.Errorf("compute minimum started machine count: %w", err)
	}
	maxStartedN, hasMaxStartedN, err := r.CalcMaxStartedMachineN()
	if err != nil {
		return fmt.Errorf("compute minimum started machine count: %w", err)
	}

	// Fetch list of running machines.
	all, err := r.listMachines(ctx)
	if err != nil {
		return fmt.Errorf("list machines: %w", err)
	}
	machines := reachbleMachines(all)
	m := machinesByState(machines)

	// Log out stats so we know exactly what the state of the world is.
	slog.Info("reconciling",
		slog.String("app", r.AppName),
		slog.Group("current",
			slog.Int("started", len(m[fly.MachineStateStarted])),
			slog.Int("stopped", len(m[fly.MachineStateStopped])),
		),
		slog.Group("target",
			slog.Group("created",
				slog.Int("min", minCreatedN),
				slog.Int("max", maxCreatedN),
			),
			slog.Group("started",
				slog.Int("min", minStartedN),
				slog.Int("max", maxStartedN),
			),
		),
	)

	// Determine if we need to create or destroy machines.
	createdN := len(machines)
	if hasMinCreatedN && createdN < minCreatedN {
		if len(machines) == 0 {
			return fmt.Errorf("no machine available to clone for scale up")
		}

		machine := machines[0]
		config := machine.Config
		config.Image = machine.FullImageRef()
		return r.createN(ctx, machines[0].Config, machine.Region, minCreatedN-createdN)
	}
	if hasMaxCreatedN && createdN > maxCreatedN {
		return r.destroyN(ctx, m, createdN-maxCreatedN)
	}

	// Determine if we need to start/stop machines.
	startedN := len(m[fly.MachineStateStarted])
	if hasMinStartedN && startedN < minStartedN {
		for startedN < minStartedN {
			toStart := minStartedN - startedN
			if r.ScaleUpStepSize > 0 {
				if time.Since(r.lastScaleUpTime) < r.ScaleUpStepInterval-time.Second*3 {
					slog.Info("not enough time has passed since last scale up, skipping",
						slog.Time("last_scale_up_time", r.lastScaleUpTime),
						slog.Time("current_time", time.Now()),
						slog.Duration("scale_up_step_interval", r.ScaleUpStepInterval),
						slog.Duration("time_since_last_scale_up", time.Since(r.lastScaleUpTime)),
					)
					break
				}
				toStart = min(r.ScaleUpStepSize, minStartedN-startedN)
			}
			if err := r.startN(ctx, m[fly.MachineStateStopped], toStart); err != nil {
				return err
			}
			startedN += toStart
			r.lastScaleUpTime = time.Now()
			if r.ScaleUpStepSize > 0 {
				// If we're scaling in steps, we don't want to start more machines
				// than the step size. We'll just try again next time.
				break
			}
		}
	}
	if hasMaxStartedN && startedN > maxStartedN {
		toStop := startedN - maxStartedN
		if r.ScaleDownStepSize > 0 {
			if time.Since(r.lastScaleDownTime) < r.ScaleDownStepInterval-time.Second*3 {
				slog.Info("not enough time has passed since last scale down, skipping",
					slog.Time("last_scale_down_time", r.lastScaleDownTime),
					slog.Time("current_time", time.Now()),
					slog.Duration("scale_down_step_interval", r.ScaleDownStepInterval),
					slog.Duration("time_since_last_scale_down", time.Since(r.lastScaleDownTime)),
				)
				return nil
			}
			r.lastScaleDownTime = time.Now()
			toStop = min(r.ScaleDownStepSize, startedN-maxStartedN)
		}
		return r.stopN(ctx, m[fly.MachineStateStarted], toStop)
	}

	r.Stats.NoScale.Add(1)
	return nil
}

func (r *Reconciler) createN(ctx context.Context, config *fly.MachineConfig, defaultRegion string, n int) error {
	r.Stats.BulkCreate.Add(1)

	logger := slog.With(slog.String("app", r.AppName))
	logger.Info("begin bulk create")

	// Attempt to start as many machines as needed.
	remaining := n
	for remaining > 0 {
		// Cycle through possible regions, if set.
		// Otherwise use the region of the source machine we're cloning.
		region := r.NextRegion()
		if region == "" {
			region = defaultRegion
		}

		machine, err := r.createMachine(ctx, config, region)
		if err != nil {
			logger.Error("cannot create machine, skipping", slog.Any("err", err))
			continue
		}

		logger.Info("machine created",
			slog.String("id", machine.ID),
			slog.String("region", machine.Region))

		remaining--
	}

	newlyCreatedN := n - remaining
	logger.Info("bulk create completed", slog.Int("n", newlyCreatedN))

	return nil
}

func (r *Reconciler) destroyN(ctx context.Context, machinesByState map[string][]*fly.Machine, n int) error {
	r.Stats.BulkDestroy.Add(1)

	logger := slog.With(slog.String("app", r.AppName))
	logger.Info("begin bulk destroy")

	// Attempt to destroy as many machines as needed.
	remaining := n
	for remaining > 0 {
		machine := chooseNextDestroyCandidate(machinesByState)
		if machine == nil {
			break
		}

		if err := r.destroyMachine(ctx, machine.ID); err != nil {
			logger.Error("cannot destroy machine, skipping", slog.Any("err", err))
			remaining-- // don't retry so we don't kill too many machines
			continue
		}

		logger.Info("machine destroyed",
			slog.String("id", machine.ID),
			slog.String("region", machine.Region))

		remaining--
	}

	newlyDestroyedN := n - remaining
	logger.Info("bulk destroy completed", slog.Int("n", newlyDestroyedN))

	return nil
}

func chooseNextDestroyCandidate(m map[string][]*fly.Machine) *fly.Machine {
	// Iterate over available machines in order of state. We want to try to
	// destroy stopped machines before destroying started machines.
	for _, state := range []string{
		fly.MachineStateStopped,
		fly.MachineStateCreated,
		fly.MachineStateStarted,
	} {
		if len(m[state]) > 0 {
			candidate := m[state][0]
			m[state] = m[state][1:] // trim machine from the front of the list
			return candidate
		}
	}

	return nil
}

func (r *Reconciler) startN(ctx context.Context, stoppedMachines []*fly.Machine, n int) error {
	r.Stats.BulkStart.Add(1)

	logger := slog.With(slog.String("app", r.AppName))
	logger.Info("begin bulk start")

	// Let the user know if we don't have enough machines to reach the target count.
	if len(stoppedMachines) < n {
		logger.Warn("not enough stopped machines available to reach target, please create more machines")
	}

	// Sort stopped machines by an arbitrary value (ID) so results are deterministic.
	sort.Slice(stoppedMachines, func(i, j int) bool { return stoppedMachines[i].ID < stoppedMachines[j].ID })

	// Attempt to start as many machines as needed.
	remaining := n
	for _, machine := range stoppedMachines {
		if remaining <= 0 {
			break
		}

		if err := r.startMachine(ctx, machine.ID); err != nil {
			logger.Error("cannot start machine, skipping",
				slog.String("id", machine.ID),
				slog.Any("err", err))
			continue
		}

		logger.Info("machine started", slog.String("id", machine.ID))
		remaining--
	}

	newlyStartedN := n - remaining
	logger.Info("bulk start completed", slog.Int("n", newlyStartedN))

	return nil
}

func (r *Reconciler) stopN(ctx context.Context, startedMachines []*fly.Machine, n int) error {
	r.Stats.BulkStop.Add(1)

	logger := slog.With(slog.String("app", r.AppName))
	logger.Info("begin bulk stop")

	// Sort stopped machines by an arbitrary value (ID) so results are deterministic.
	sort.Slice(startedMachines, func(i, j int) bool { return startedMachines[i].ID < startedMachines[j].ID })

	// Attempt to stop as many machines as needed.
	remaining := n
	for _, machine := range startedMachines {
		if remaining <= 0 {
			break
		}

		if err := r.stopMachine(ctx, machine.ID); err != nil {
			logger.Error("cannot stop machine, skipping",
				slog.String("id", machine.ID),
				slog.Any("err", err))
			continue
		}

		logger.Info("machine stopped", slog.String("id", machine.ID))
		remaining--
	}

	newlyStoppedN := n - remaining
	logger.Info("bulk stop completed", slog.Int("n", newlyStoppedN))

	return nil
}

func (r *Reconciler) listMachines(ctx context.Context) ([]*fly.Machine, error) {
	machines, err := r.Client.List(ctx, "")
	if err != nil {
		return nil, err
	}
	return machines, nil
}

func (r *Reconciler) createMachine(ctx context.Context, config *fly.MachineConfig, region string) (*fly.Machine, error) {
	machine, err := r.Client.Launch(ctx, fly.LaunchMachineInput{
		Config:     config,
		Region:     region,
		SkipLaunch: r.InitialMachineState == fly.MachineStateStopped,
	})
	if err != nil {
		r.Stats.MachineCreateFailed.Add(1)
		return nil, err
	}
	r.Stats.MachineCreated.Add(1)
	return machine, nil
}

func (r *Reconciler) destroyMachine(ctx context.Context, id string) error {
	if err := r.Client.Destroy(ctx, fly.RemoveMachineInput{ID: id, Kill: true}, ""); err != nil {
		r.Stats.MachineDestroyFailed.Add(1)
		return err
	}
	r.Stats.MachineDestroyed.Add(1)
	return nil
}

func (r *Reconciler) startMachine(ctx context.Context, id string) error {
	if _, err := r.Client.Start(ctx, id, ""); err != nil {
		r.Stats.MachineStartFailed.Add(1)
		return err
	}
	r.Stats.MachineStarted.Add(1)
	return nil
}

func (r *Reconciler) stopMachine(ctx context.Context, id string) error {
	if err := r.Client.Stop(ctx, fly.StopMachineInput{ID: id}, ""); err != nil {
		r.Stats.MachineStopFailed.Add(1)
		return err
	}
	r.Stats.MachineStopped.Add(1)
	return nil
}

// CalcMinCreatedMachineN returns the minimum number of created machines.
func (r *Reconciler) CalcMinCreatedMachineN() (int, bool, error) {
	v, ok, err := r.evalInt(r.MinCreatedMachineN)
	if err != nil || !ok {
		return v, ok, err
	}

	// We cannot scale to zero as we will not have a machine available to clone
	// on the creation phase of scaling up.
	if v <= 1 {
		v = 1
	}
	return v, true, nil
}

// CalcMaxCreatedMachineN returns the maximum number of created machines.
func (r *Reconciler) CalcMaxCreatedMachineN() (int, bool, error) {
	v, ok, err := r.evalInt(r.MaxCreatedMachineN)
	if err != nil || !ok {
		return v, ok, err
	}

	// We cannot scale to zero as we will not have a machine available to clone
	// on the creation phase of scaling up.
	if v <= 1 {
		v = 1
	}
	return v, true, nil
}

// CalcMinStartedMachineN returns the minimum number of started machines.
func (r *Reconciler) CalcMinStartedMachineN() (int, bool, error) {
	return r.evalInt(r.MinStartedMachineN)
}

// CalcMaxStartedMachineN returns the maximum number of started machines.
func (r *Reconciler) CalcMaxStartedMachineN() (int, bool, error) {
	return r.evalInt(r.MaxStartedMachineN)
}

// evalInt compiles & runs an expression. Returns a rounded integer.
// Returns a true if the second argument if s is not blank. Otherwise returns false.
func (r *Reconciler) evalInt(s string) (int, bool, error) {
	if s == "" {
		return 0, false, nil
	}

	env := map[string]any{}
	for k, v := range r.metrics {
		env[k] = v
	}

	program, err := expr.Compile(s, expr.AsFloat64(), expr.Env(env))
	if err != nil {
		return 0, true, fmt.Errorf("compile expression: %w", err)
	}

	v, err := expr.Run(program, env)
	if err != nil {
		return 0, true, fmt.Errorf("execute expression: %w", err)
	}

	f := math.Round(v.(float64))
	if math.IsNaN(f) {
		return 0, true, ErrExprNaN
	} else if math.IsInf(f, 0) {
		return 0, true, ErrExprInf
	}

	if f < 0 {
		return 0, true, nil
	}
	return int(f), true, nil
}

func machinesByState(a []*fly.Machine) map[string][]*fly.Machine {
	m := make(map[string][]*fly.Machine)
	for _, mach := range a {
		m[mach.State] = append(m[mach.State], mach)
	}
	return m
}

type ReconcilerStats struct {
	// Outcomes, incremented for each reconciliation.
	BulkCreate  atomic.Int64
	BulkDestroy atomic.Int64
	BulkStart   atomic.Int64
	BulkStop    atomic.Int64
	NoScale     atomic.Int64

	// Individual machine stats.
	MachineCreated       atomic.Int64
	MachineCreateFailed  atomic.Int64
	MachineDestroyed     atomic.Int64
	MachineDestroyFailed atomic.Int64
	MachineStarted       atomic.Int64
	MachineStartFailed   atomic.Int64
	MachineStopped       atomic.Int64
	MachineStopFailed    atomic.Int64
}
