package main

import (
	"context"
	"flag"
	"github.com/xencodes/cadence-sample/utils"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"log"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var Domain = "simple-domain"
var TaskListName = "SimpleWorker"

func main() {
	svcClient := utils.NewServiceClient()
	logger := utils.NewLogger()

	var mode string
	flag.StringVar(&mode, "m", "worker", "trigger / worker")
	flag.Parse()

	switch mode {
	case "worker":
		startWorker(logger, svcClient)
		select {}
	case "trigger":
		cli := client.NewClient(svcClient, Domain, &client.Options{})

		_, err := cli.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
			TaskList:                     TaskListName,
			ExecutionStartToCloseTimeout: time.Second * 1,
		}, SimpleWorkflow, "MyArgument")
		if err != nil {
			panic(err)
		}
	}
}

func startWorker(logger *zap.Logger, service workflowserviceclient.Interface) {
	// TaskListName identifies set of client workflows, activities, and workers.
	// It could be your group or client or application name.
	workerOptions := worker.Options{
		Logger:       logger,
		MetricsScope: tally.NewTestScope(TaskListName, map[string]string{}),
	}

	w := worker.New(
		service,
		Domain,
		TaskListName,
		workerOptions)

	w.RegisterWorkflowWithOptions(SimpleWorkflow, workflow.RegisterOptions{Name: "SimpleWorkflow"})
	w.RegisterActivityWithOptions(SimpleActivity, activity.RegisterOptions{Name: "SimpleActivity", EnableAutoHeartbeat: true})

	err := w.Start()
	if err != nil {
		panic("Failed to start w")
	}

	logger.Info("Started Worker.", zap.String("w", TaskListName))
}

func SimpleWorkflow(ctx workflow.Context, value string) error {
	ao := workflow.ActivityOptions{
		TaskList:               TaskListName,
		ScheduleToStartTimeout: time.Second * 1,
		StartToCloseTimeout:    time.Minute * 5,
		HeartbeatTimeout:       time.Minute * 3,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	future := workflow.ExecuteActivity(ctx, SimpleActivity, value)
	var result string
	if err := future.Get(ctx, &result); err != nil {
		panic(err)
	}
	workflow.GetLogger(ctx).Info("Done", zap.String("result", result))
	return nil
}

func SimpleActivity(ctx context.Context, value string) (string, error) {
	log.Println("SimpleActivity", value)
	return "Processed: " + value, nil
}
