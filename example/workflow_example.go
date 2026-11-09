package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/goptics/sqliteq"
	"github.com/goptics/varmq"
)

// workflowExample runs a two-step workflow over two queues. The state lives
// in __sqliteq_workflows.db next to the queue files.
func workflowExample() {
	dir, err := os.MkdirTemp("", "sqliteq-workflow-*")
	if err != nil {
		log.Fatalf("Failed to create temp folder: %v", err)
	}
	defer os.RemoveAll(dir)

	qs, err := sqliteq.New(dir)
	if err != nil {
		log.Fatalf("Failed to open queues: %v", err)
	}
	defer qs.Close()

	resize, err := qs.NewQueue("resize")
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	publish, err := qs.NewQueue("publish")
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	// The store registers itself for the scope of the queues
	store, err := sqliteq.NewWorkflowStore(qs)
	if err != nil {
		log.Fatalf("Failed to create workflow store: %v", err)
	}
	defer store.Close()

	step := func(name string) func(varmq.Job[string]) error {
		return func(j varmq.Job[string]) error {
			fmt.Printf("%s %s\n", name, j.Data())
			return nil
		}
	}
	resizer := varmq.NewErrWorker(step("resize"))
	defer resizer.WaitAndStop()
	publisher := varmq.NewErrWorker(step("publish"))
	defer publisher.WaitAndStop()

	wf := varmq.NewWorkflow(store)
	a := wf.Add("resize", resizer.WithQueue(resize), "photo.jpg")
	wf.Add("publish", publisher.WithQueue(publish), "photo.jpg").After(a)

	if err := wf.Submit(context.Background()); err != nil {
		log.Fatalf("Failed to submit workflow: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	record, err := wf.WaitContext(ctx)
	if err != nil {
		log.Fatalf("Workflow did not finish: %v", err)
	}
	fmt.Println("workflow", record.ID, record.State)
}
