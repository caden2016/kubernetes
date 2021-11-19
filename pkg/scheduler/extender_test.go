/*
Copyright 2015 The Kubernetes Authors.

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

package scheduler

import (
	"context"
	"k8s.io/kubernetes/pkg/scheduler/framework/fake"
	"reflect"
	"testing"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	internalcache "k8s.io/kubernetes/pkg/scheduler/internal/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/internal/queue"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

func TestGenericSchedulerWithExtenders(t *testing.T) {
	tests := []struct {
		name            string
		registerPlugins []st.RegisterPluginFunc
		extenders       []st.FakeExtender
		nodes           []string
		expectedResult  ScheduleResult
		expectsErr      bool
	}{
		{
			registerPlugins: []st.RegisterPluginFunc{
				st.RegisterFilterPlugin("TrueFilter", st.NewTrueFilterPlugin),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []st.FakeExtender{
				{
					Predicates: []st.FitPredicate{st.TruePredicateExtender},
				},
				{
					Predicates: []st.FitPredicate{st.ErrorPredicateExtender},
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: true,
			name:       "test 1",
		},
		{
			registerPlugins: []st.RegisterPluginFunc{
				st.RegisterFilterPlugin("TrueFilter", st.NewTrueFilterPlugin),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []st.FakeExtender{
				{
					Predicates: []st.FitPredicate{st.TruePredicateExtender},
				},
				{
					Predicates: []st.FitPredicate{st.FalsePredicateExtender},
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: true,
			name:       "test 2",
		},
		{
			registerPlugins: []st.RegisterPluginFunc{
				st.RegisterFilterPlugin("TrueFilter", st.NewTrueFilterPlugin),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []st.FakeExtender{
				{
					Predicates: []st.FitPredicate{st.TruePredicateExtender},
				},
				{
					Predicates: []st.FitPredicate{st.Node1PredicateExtender},
				},
			},
			nodes: []string{"node1", "node2"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node1",
				EvaluatedNodes: 2,
				FeasibleNodes:  1,
			},
			name: "test 3",
		},
		{
			registerPlugins: []st.RegisterPluginFunc{
				st.RegisterFilterPlugin("TrueFilter", st.NewTrueFilterPlugin),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []st.FakeExtender{
				{
					Predicates: []st.FitPredicate{st.Node2PredicateExtender},
				},
				{
					Predicates: []st.FitPredicate{st.Node1PredicateExtender},
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: true,
			name:       "test 4",
		},
		{
			registerPlugins: []st.RegisterPluginFunc{
				st.RegisterFilterPlugin("TrueFilter", st.NewTrueFilterPlugin),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []st.FakeExtender{
				{
					Predicates:   []st.FitPredicate{st.TruePredicateExtender},
					Prioritizers: []st.PriorityConfig{{Function: st.ErrorPrioritizerExtender, Weight: 10}},
					Weight:       1,
				},
			},
			nodes: []string{"node1"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node1",
				EvaluatedNodes: 1,
				FeasibleNodes:  1,
			},
			name: "test 5",
		},
		{
			registerPlugins: []st.RegisterPluginFunc{
				st.RegisterFilterPlugin("TrueFilter", st.NewTrueFilterPlugin),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []st.FakeExtender{
				{
					Predicates:   []st.FitPredicate{st.TruePredicateExtender},
					Prioritizers: []st.PriorityConfig{{Function: st.Node1PrioritizerExtender, Weight: 10}},
					Weight:       1,
				},
				{
					Predicates:   []st.FitPredicate{st.TruePredicateExtender},
					Prioritizers: []st.PriorityConfig{{Function: st.Node2PrioritizerExtender, Weight: 10}},
					Weight:       5,
				},
			},
			nodes: []string{"node1", "node2"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node2",
				EvaluatedNodes: 2,
				FeasibleNodes:  2,
			},
			name: "test 6",
		},
		{
			registerPlugins: []st.RegisterPluginFunc{
				st.RegisterFilterPlugin("TrueFilter", st.NewTrueFilterPlugin),
				st.RegisterScorePlugin("Node2Prioritizer", st.NewNode2PrioritizerPlugin(), 20),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []st.FakeExtender{
				{
					Predicates:   []st.FitPredicate{st.TruePredicateExtender},
					Prioritizers: []st.PriorityConfig{{Function: st.Node1PrioritizerExtender, Weight: 10}},
					Weight:       1,
				},
			},
			nodes: []string{"node1", "node2"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node2",
				EvaluatedNodes: 2,
				FeasibleNodes:  2,
			}, // node2 has higher score
			name: "test 7",
		},
		{
			// Scheduler is expected to not send pod to extender in
			// Filter/Prioritize phases if the extender is not interested in
			// the pod.
			//
			// If scheduler sends the pod by mistake, the test would fail
			// because of the errors from errorPredicateExtender and/or
			// errorPrioritizerExtender.
			registerPlugins: []st.RegisterPluginFunc{
				st.RegisterFilterPlugin("TrueFilter", st.NewTrueFilterPlugin),
				st.RegisterScorePlugin("Node2Prioritizer", st.NewNode2PrioritizerPlugin(), 1),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []st.FakeExtender{
				{
					Predicates:   []st.FitPredicate{st.ErrorPredicateExtender},
					Prioritizers: []st.PriorityConfig{{Function: st.ErrorPrioritizerExtender, Weight: 10}},
					UnInterested: true,
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: false,
			expectedResult: ScheduleResult{
				SuggestedHost:  "node2",
				EvaluatedNodes: 2,
				FeasibleNodes:  2,
			}, // node2 has higher score
			name: "test 8",
		},
		{
			// Scheduling is expected to not fail in
			// Filter/Prioritize phases if the extender is not available and ignorable.
			//
			// If scheduler did not ignore the extender, the test would fail
			// because of the errors from errorPredicateExtender.
			registerPlugins: []st.RegisterPluginFunc{
				st.RegisterFilterPlugin("TrueFilter", st.NewTrueFilterPlugin),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []st.FakeExtender{
				{
					Predicates: []st.FitPredicate{st.ErrorPredicateExtender},
					Ignorable:  true,
				},
				{
					Predicates: []st.FitPredicate{st.Node1PredicateExtender},
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: false,
			expectedResult: ScheduleResult{
				SuggestedHost:  "node1",
				EvaluatedNodes: 2,
				FeasibleNodes:  1,
			},
			name: "test 9",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := clientsetfake.NewSimpleClientset()
			informerFactory := informers.NewSharedInformerFactory(client, 0)

			var extenders []framework.Extender
			for ii := range test.extenders {
				extenders = append(extenders, &test.extenders[ii])
			}
			cache := internalcache.New(time.Duration(0), wait.NeverStop)
			for _, name := range test.nodes {
				cache.AddNode(createNode(name))
			}
			fwk, err := st.NewFramework(
				test.registerPlugins, "",
				runtime.WithClientSet(client),
				runtime.WithInformerFactory(informerFactory),
				runtime.WithPodNominator(internalqueue.NewPodNominator(informerFactory.Core().V1().Pods().Lister())),
			)
			if err != nil {
				t.Fatal(err)
			}

			scheduler := NewGenericScheduler(
				cache,
				emptySnapshot,
				schedulerapi.DefaultPercentageOfNodesToScore)
			podIgnored := &v1.Pod{}
			result, err := scheduler.Schedule(context.Background(), extenders, fwk, framework.NewCycleState(), podIgnored)
			if test.expectsErr {
				if err == nil {
					t.Errorf("Unexpected non-error, result %+v", result)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
					return
				}

				if !reflect.DeepEqual(result, test.expectedResult) {
					t.Errorf("Expected: %+v, Saw: %+v", test.expectedResult, result)
				}
			}
		})
	}
}

func createNode(name string) *v1.Node {
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func TestIsInterested(t *testing.T) {
	mem := &HTTPExtender{
		managedResources: sets.NewString(),
	}
	mem.managedResources.Insert("memory")

	for _, tc := range []struct {
		label    string
		extender *HTTPExtender
		pod      *v1.Pod
		want     bool
	}{
		{
			label: "Empty managed resources",
			extender: &HTTPExtender{
				managedResources: sets.NewString(),
			},
			pod:  &v1.Pod{},
			want: true,
		},
		{
			label:    "Managed memory, empty resources",
			extender: mem,
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "app",
						},
					},
				},
			},
			want: false,
		},
		{
			label:    "Managed memory, container memory",
			extender: mem,
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "app",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{"memory": resource.Quantity{}},
								Limits:   v1.ResourceList{"memory": resource.Quantity{}},
							},
						},
					},
				},
			},
			want: true,
		},
		{
			label:    "Managed memory, init container memory",
			extender: mem,
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "app",
						},
					},
					InitContainers: []v1.Container{
						{
							Name: "init",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{"memory": resource.Quantity{}},
								Limits:   v1.ResourceList{"memory": resource.Quantity{}},
							},
						},
					},
				},
			},
			want: true,
		},
	} {
		t.Run(tc.label, func(t *testing.T) {
			if got := tc.extender.IsInterested(tc.pod); got != tc.want {
				t.Fatalf("IsInterested(%v) = %v, wanted %v", tc.pod, got, tc.want)
			}
		})
	}
}

// TestRestoreNumPDBViolations test functions convertToNodeNameToMetaVictims and HTTPExtender.convertToNodeNameToVictims in HTTPExtender.ProcessPreemption
// Ensure the NumPDBViolations of each node is returned from external server.
func TestRestoreNumPDBViolations(t *testing.T) {
	se := &HTTPExtender{}
	nodes := []string{"node1", "node2"}
	podsInNodeList := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod1", UID: "uid1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod2", UID: "uid2"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod3", UID: "uid3"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod4", UID: "uid4"}},
	}

	nodeInfoList := make([]*framework.NodeInfo, 0, len(nodes))
	nodeNameToVictims := make(map[string]*extenderv1.Victims)
	result := extenderv1.ExtenderPreemptionResult{
		make(map[string]*extenderv1.MetaVictims),
	}

	// nodeNameToVictims{node1:{Pods: [pod1,pod3], NumPDBViolations: 1}, node2:{Pods: [pod2,pod4], NumPDBViolations: 2}}
	// nodeInfoList[{node1:{Pods: pod1,pod3}}, {node2:{Pods: pod2,pod4}}]
	// result.NodeNameToMetaVictims {node1:{MetaPod: [uid1,uid3], NumPDBViolations: 1}, node2:{MetaPod: [uid2,uid4], NumPDBViolations: 2}}
	for i, nm := range nodes {
		nodeInfo := framework.NewNodeInfo()
		node := createNode(nm)
		nodeInfo.SetNode(node)
		nodeInfo.AddPod(podsInNodeList[i])
		nodeInfo.AddPod(podsInNodeList[i+2])
		nodeInfoList = append(nodeInfoList, nodeInfo)

		victims := &extenderv1.Victims{NumPDBViolations: 1 + int64(i)}
		victims.Pods = append(victims.Pods, podsInNodeList[i])
		victims.Pods = append(victims.Pods, podsInNodeList[i+2])
		nodeNameToVictims[nm] = victims

		result.NodeNameToMetaVictims[nm] = &extenderv1.MetaVictims{NumPDBViolations: 1 + int64(i)}
		result.NodeNameToMetaVictims[nm].Pods = append(result.NodeNameToMetaVictims[nm].Pods, &extenderv1.MetaPod{UID: string(podsInNodeList[i].UID)})
		result.NodeNameToMetaVictims[nm].Pods = append(result.NodeNameToMetaVictims[nm].Pods, &extenderv1.MetaPod{UID: string(podsInNodeList[i+2].UID)})
	}

	for _, tc := range []struct {
		label            string
		nodeCacheCapable bool
	}{
		{
			label:            "nodeCacheCapable enabled",
			nodeCacheCapable: true,
		},
		{
			label:            "nodeCacheCapable disenabled",
			nodeCacheCapable: false,
		},
	} {
		t.Run(tc.label, func(t *testing.T) {

			// NodeCacheCapable enabled, check func convertToNodeNameToMetaVictims
			// make sure  NumPDBViolations is transfered from nodeNameToVictims to nodeNameToMetaVictims
			// Then the custom server should send it back to HTTPExtender, if it do not care or change it.
			if tc.nodeCacheCapable {
				nodeNameToMetaVictims := convertToNodeNameToMetaVictims(nodeNameToVictims)
				for nm, victims := range nodeNameToVictims {
					if victims.NumPDBViolations != nodeNameToMetaVictims[nm].NumPDBViolations {
						t.Fatalf("convertToNodeNameToMetaVictims got node:%s NumPDBViolations:%d, want:%d ", nm, nodeNameToMetaVictims[nm].NumPDBViolations, victims.NumPDBViolations)
					}
				}
			}

			// Check func HTTPExtender.convertToNodeNameToVictims
			// Make sure the NumPDBViolations is transfered from result.NodeNameToMetaVictims to newNodeNameToVictims
			newNodeNameToVictims, err := se.convertToNodeNameToVictims(result.NodeNameToMetaVictims, fake.NodeInfoLister(nodeInfoList))
			if err != nil {
				t.Fatalf("HTTPExtender.convertToNodeNameToVictims got err:%v", err)
			}
			for nm, victims := range result.NodeNameToMetaVictims {
				if victims.NumPDBViolations != newNodeNameToVictims[nm].NumPDBViolations {
					t.Fatalf("HTTPExtender.convertToNodeNameToVictims got node:%s NumPDBViolations:%d, want:%d ", nm, newNodeNameToVictims[nm].NumPDBViolations, victims.NumPDBViolations)
				}
			}
		})
	}

}
