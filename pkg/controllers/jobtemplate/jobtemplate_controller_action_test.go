/*
Copyright 2022 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package jobtemplate

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	kubeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"volcano.sh/apis/pkg/apis/batch/v1alpha1"
	jobflowv1alpha1 "volcano.sh/apis/pkg/apis/flow/v1alpha1"
	volcanoclient "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	informerfactory "volcano.sh/apis/pkg/client/informers/externalversions"
	"volcano.sh/volcano/pkg/controllers/apis"
	"volcano.sh/volcano/pkg/controllers/framework"
)

func newFakeController() *jobtemplatecontroller {
	volcanoClientSet := volcanoclient.NewSimpleClientset()
	kubeClientSet := kubeclient.NewSimpleClientset()

	sharedInformers := informers.NewSharedInformerFactory(kubeClientSet, 0)
	vcSharedInformers := informerfactory.NewSharedInformerFactory(volcanoClientSet, 0)

	controller := &jobtemplatecontroller{}
	opt := &framework.ControllerOption{
		VolcanoClient:           volcanoClientSet,
		KubeClient:              kubeClientSet,
		SharedInformerFactory:   sharedInformers,
		VCSharedInformerFactory: vcSharedInformers,
		WorkerNum:               3,
	}

	controller.Initialize(opt)

	return controller
}

func TestSyncJobTemplateFunc(t *testing.T) {
	namespace := "test"

	type args struct {
		jobTemplate *jobflowv1alpha1.JobTemplate
		jobList     []*v1alpha1.Job
	}
	type wantRes struct {
		jobTemplateStatus *jobflowv1alpha1.JobTemplateStatus
		err               error
	}
	tests := []struct {
		name string
		args args
		want wantRes
	}{
		{
			name: "SyncJobTemplate success case",
			args: args{
				jobTemplate: &jobflowv1alpha1.JobTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "jobtemplate",
						Namespace: namespace,
					},
					Spec: v1alpha1.JobSpec{},
				},
				jobList: []*v1alpha1.Job{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:        "job1",
							Namespace:   namespace,
							Labels:      map[string]string{CreatedByJobTemplate: GetTemplateString(namespace, "jobtemplate")},
							Annotations: map[string]string{CreatedByJobTemplate: GetTemplateString(namespace, "jobtemplate")},
						},
						Spec:   v1alpha1.JobSpec{},
						Status: v1alpha1.JobStatus{},
					},
				},
			},
			want: wantRes{
				jobTemplateStatus: &jobflowv1alpha1.JobTemplateStatus{
					JobDependsOnList: []string{"job1"},
				},
				err: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeController := newFakeController()

			for i := range tt.args.jobList {
				if err := fakeController.jobInformer.Informer().GetIndexer().Add(tt.args.jobList[i]); err != nil {
					t.Errorf("add vcjob to informerFake,error : %s", err.Error())
				}
			}

			if _, err := fakeController.vcClient.FlowV1alpha1().JobTemplates(namespace).Create(context.Background(), tt.args.jobTemplate, metav1.CreateOptions{}); err != nil {
				t.Errorf("create jobTemplate failed,error : %s", err.Error())
			}

			if got := fakeController.syncJobTemplate(tt.args.jobTemplate); got != tt.want.err {
				t.Error("Expected deleteAllJobsCreateByJobFlow() return nil, but not nil")
			}
			if !equality.Semantic.DeepEqual(&tt.args.jobTemplate.Status, tt.want.jobTemplateStatus) {
				t.Error("not the expected result")
			}
		})
	}
}

func TestDeletedJobReconcilesTemplate(t *testing.T) {
	controller := newFakeController()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	template := &jobflowv1alpha1.JobTemplate{ObjectMeta: metav1.ObjectMeta{Name: "train", Namespace: "team"}}
	if _, err := controller.vcClient.FlowV1alpha1().JobTemplates("team").Create(ctx, template, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	job := &v1alpha1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "pipeline-train", Namespace: "team",
		Labels: map[string]string{CreatedByJobTemplate: "team.train"},
	}}
	if _, err := controller.vcClient.BatchV1alpha1().Jobs("team").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { controller.Run(stop); close(done) }()
	defer func() { close(stop); <-done }()
	waitForCount := func(want int) {
		t.Helper()
		err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
			got, err := controller.vcClient.FlowV1alpha1().JobTemplates("team").Get(ctx, "train", metav1.GetOptions{})
			return err == nil && len(got.Status.JobDependsOnList) == want, err
		})
		if err != nil {
			t.Fatalf("template did not reach %d job references: %v", want, err)
		}
	}
	waitForCount(1)
	if err := controller.vcClient.BatchV1alpha1().Jobs("team").Delete(ctx, job.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForCount(0)
}

func TestDeleteJobTombstone(t *testing.T) {
	var got []apis.FlowRequest
	controller := &jobtemplatecontroller{enqueueJobTemplate: func(req apis.FlowRequest) { got = append(got, req) }}
	job := &v1alpha1.Job{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{CreatedByJobTemplate: "team.train"}}}
	controller.deleteJob(cache.DeletedFinalStateUnknown{Key: "team/pipeline-train", Obj: job})
	if len(got) != 1 || got[0].Namespace != "team" || got[0].JobTemplateName != "train" {
		t.Errorf("unexpected requests: %+v", got)
	}
}
