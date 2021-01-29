/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Summary", func() {

	Context("extractIntValue", func() {
		dict := map[string]interface{}{
			"key1": 123,
			"key2": int8(54),
			"key3": "456",
			"key4": "456.123",
			"key5": "some non-number value",
			"key6": int64(12456),
		}

		When("map is nil", func() {
			extracted := extractIntValue(nil, "key", -1)

			It("should return default value", func() {
				Expect(extracted).To(BeNumerically("==", -1))
			})
		})

		When("given key does not exist", func() {
			extracted := extractIntValue(&dict, "key", -1)

			It("should return default value if key not found", func() {
				Expect(extracted).To(BeNumerically("==", -1))
			})
		})

		When("given key is mapped to an int value", func() {
			extracted := extractIntValue(&dict, "key1", -1)

			It("should return the int value", func() {
				Expect(extracted).To(BeNumerically("==", 123))
			})
		})

		When("given key is mapped to an int64 value", func() {
			extracted := extractIntValue(&dict, "key6", -1)

			It("should return the int value", func() {
				Expect(extracted).To(BeNumerically("==", 12456))
			})
		})

		When("given key is mapped to a byte value", func() {
			extracted := extractIntValue(&dict, "key2", -1)

			It("should return the int value", func() {
				Expect(extracted).To(BeNumerically("==", 54))
			})
		})

		When("given key is mapped to an int string", func() {
			extracted := extractIntValue(&dict, "key3", -1)

			It("should return the int value", func() {
				Expect(extracted).To(BeNumerically("==", 456))
			})
		})

		When("given key is mapped to a float string", func() {
			extracted := extractIntValue(&dict, "key4", -1)

			It("should return default value", func() {
				Expect(extracted).To(BeNumerically("==", -1))
			})
		})

		When("given key is mapped to a string", func() {
			extracted := extractIntValue(&dict, "key5", -1)

			It("should return default value", func() {
				Expect(extracted).To(BeNumerically("==", -1))
			})
		})
	})

	Context("extractStringValue", func() {
		dict := map[string]interface{}{
			"key1": "123",
			"key2": 456,
			"key3": 3 * time.Second,
			"key4": 123.568,
			"key5": nil,
		}

		When("map is nil", func() {
			extracted := extractStringValue(nil, "key", "default")

			It("should return default value", func() {
				Expect(extracted).To(Equal("default"))
			})
		})

		When("given key does not exist", func() {
			extracted := extractStringValue(&dict, "key", "default")

			It("should return default value", func() {
				Expect(extracted).To(Equal("default"))
			})
		})

		When("given key is mapped to nil", func() {
			extracted := extractStringValue(&dict, "key5", "default")

			It("should return the default value", func() {
				Expect(extracted).To(Equal("default"))
			})
		})

		When("given key is mapped to a string value", func() {
			extracted := extractStringValue(&dict, "key1", "default")

			It("should return the string value", func() {
				Expect(extracted).To(Equal("123"))
			})
		})

		When("given key is mapped to an int string", func() {
			extracted := extractStringValue(&dict, "key2", "default")

			It("should return the converted string value", func() {
				Expect(extracted).To(Equal("456"))
			})
		})

		When("given key is mapped to a duration string", func() {
			extracted := extractStringValue(&dict, "key3", "default")

			It("should return the converted string value", func() {
				Expect(extracted).To(Equal("3s"))
			})
		})

		When("given key is mapped to a float string", func() {
			extracted := extractStringValue(&dict, "key4", "default")

			It("should return the converted string value", func() {
				Expect(extracted).To(Equal("123.568"))
			})
		})
	})

	Context("collectCounters", func() {
		When("map is nil", func() {
			It("should not collect anything", func() {
				counters := collectCounters(nil)
				Expect(counters).To(BeEquivalentTo(&neoCounters{}))
			})
		})

		When("map contains counter values", func() {
			dict := map[string]interface{}{
				"nodes-created":         1,
				"nodes-deleted":         2,
				"relationships-created": 3,
				"relationships-deleted": 4,
				"properties-set":        5,
				"labels-added":          6,
				"labels-removed":        7,
				"indexes-added":         8,
				"indexes-removed":       9,
				"constraints-added":     10,
				"constraints-removed":   11,
			}

			counters := collectCounters(&dict)

			It("should have NodesCreated = 1", func() {
				Expect(counters.NodesCreated()).To(BeNumerically("==", 1))
			})

			It("should have NodesDeleted = 2", func() {
				Expect(counters.NodesDeleted()).To(BeNumerically("==", 2))
			})

			It("should have RelationshipsCreated = 3", func() {
				Expect(counters.RelationshipsCreated()).To(BeNumerically("==", 3))
			})

			It("should have RelationshipsDeleted = 4", func() {
				Expect(counters.RelationshipsDeleted()).To(BeNumerically("==", 4))
			})

			It("should have PropertiesSet = 5", func() {
				Expect(counters.PropertiesSet()).To(BeNumerically("==", 5))
			})

			It("should have LabelsAdded = 6", func() {
				Expect(counters.LabelsAdded()).To(BeNumerically("==", 6))
			})

			It("should have LabelsRemoved = 7", func() {
				Expect(counters.LabelsRemoved()).To(BeNumerically("==", 7))
			})

			It("should have IndexesAdded = 8", func() {
				Expect(counters.IndexesAdded()).To(BeNumerically("==", 8))
			})

			It("should have IndexesRemoved = 9", func() {
				Expect(counters.IndexesRemoved()).To(BeNumerically("==", 9))
			})

			It("should have ConstraintsAdded = 10", func() {
				Expect(counters.ConstraintsAdded()).To(BeNumerically("==", 10))
			})

			It("should have ConstraintsRemoved = 11", func() {
				Expect(counters.ConstraintsRemoved()).To(BeNumerically("==", 11))
			})
		})
	})

	Context("collectNotifications", func() {
		When("map is nil", func() {
			It("should not collect anything", func() {
				var target *[]Notification = nil
				collectNotification(nil, target)
				Expect(target).To(BeNil())
			})
		})

		When("map contains one notification", func() {
			list := []interface{}{
				map[string]interface{}{
					"code":        "c1",
					"title":       "t1",
					"description": "d1",
					"position": map[string]interface{}{
						"offset": 1,
						"line":   2,
						"column": 3,
					},
					"severity": "s1",
				},
			}
			var notifications []Notification
			collectNotification(&list, &notifications)

			It("should have 1 item", func() {
				Expect(notifications).To(HaveLen(1))
			})

			Context("Item 0", func() {
				item := notifications[0]

				It("should have Code = `c1`", func() {
					Expect(item.Code()).To(BeIdenticalTo("c1"))
				})

				It("should have Title = `t1`", func() {
					Expect(item.Title()).To(BeIdenticalTo("t1"))
				})

				It("should have Description = `d1`", func() {
					Expect(item.Description()).To(BeIdenticalTo("d1"))
				})

				Context("Position", func() {
					It("should have Offset = 1", func() {
						Expect(item.Position().Offset()).To(BeNumerically("==", 1))
					})

					It("should have Line = 2", func() {
						Expect(item.Position().Line()).To(BeNumerically("==", 2))
					})

					It("should have Column = 3", func() {
						Expect(item.Position().Column()).To(BeNumerically("==", 3))
					})
				})

				It("should have Severity = `s1`", func() {
					Expect(item.Severity()).To(BeIdenticalTo("s1"))
				})
			})
		})

		When("map contains two notifications", func() {
			list := []interface{}{
				map[string]interface{}{
					"code":        "c1",
					"title":       "t1",
					"description": "d1",
					"position": map[string]interface{}{
						"offset": 1,
						"line":   2,
						"column": 3,
					},
					"severity": "s1",
				},
				map[string]interface{}{
					"code":        "c2",
					"title":       "t2",
					"description": "d2",
					"position": map[string]interface{}{
						"offset": 4,
						"line":   5,
						"column": 6,
					},
					"severity": "s2",
				},
			}
			var notifications []Notification
			collectNotification(&list, &notifications)

			It("should have 2 items", func() {
				Expect(notifications).To(HaveLen(2))
			})

			Context("Item 0", func() {
				item := notifications[0]

				It("should have Code = `c1`", func() {
					Expect(item.Code()).To(BeIdenticalTo("c1"))
				})

				It("should have Title = `t1`", func() {
					Expect(item.Title()).To(BeIdenticalTo("t1"))
				})

				It("should have Description = `d1`", func() {
					Expect(item.Description()).To(BeIdenticalTo("d1"))
				})

				Context("Position", func() {
					It("should have Offset = 1", func() {
						Expect(item.Position().Offset()).To(BeNumerically("==", 1))
					})

					It("should have Line = 2", func() {
						Expect(item.Position().Line()).To(BeNumerically("==", 2))
					})

					It("should have Column = 3", func() {
						Expect(item.Position().Column()).To(BeNumerically("==", 3))
					})
				})

				It("should have Severity = `s1`", func() {
					Expect(item.Severity()).To(BeIdenticalTo("s1"))
				})
			})

			Context("Item 1", func() {
				item := notifications[1]

				It("should have Code = `c2`", func() {
					Expect(item.Code()).To(BeIdenticalTo("c2"))
				})

				It("should have Title = `t2`", func() {
					Expect(item.Title()).To(BeIdenticalTo("t2"))
				})

				It("should have Description = `d2`", func() {
					Expect(item.Description()).To(BeIdenticalTo("d2"))
				})

				Context("Position", func() {
					It("should have Offset = 4", func() {
						Expect(item.Position().Offset()).To(BeNumerically("==", 4))
					})

					It("should have Line = 5", func() {
						Expect(item.Position().Line()).To(BeNumerically("==", 5))
					})

					It("should have Column = 6", func() {
						Expect(item.Position().Column()).To(BeNumerically("==", 6))
					})
				})

				It("should have Severity = `s2`", func() {
					Expect(item.Severity()).To(BeIdenticalTo("s2"))
				})
			})
		})
	})

	Context("collectPlan", func() {
		When("given map is nil", func() {
			It("should not collect anything", func() {
				plan := collectPlan(nil)
				Expect(plan).To(BeNil())
			})
		})

		When("given map contains a plan without children", func() {
			dict := map[string]interface{}{
				"operatorType": "XOperator",
				"args": map[string]interface{}{
					"x": 1,
					"y": "string",
					"z": 0.15,
				},
				"identifiers": []interface{}{"id1", "id2"},
			}

			planPtr := collectPlan(&dict)

			It("should not be nil", func() {
				Expect(planPtr).NotTo(BeNil())
			})

			It("should have Operator = XOperator", func() {
				Expect(planPtr.Operator()).To(BeIdenticalTo("XOperator"))
			})

			Context("Args", func() {
				It("should have 3 items", func() {
					Expect(planPtr.Arguments()).To(HaveLen(3))
				})

				It("should contain x = 1", func() {
					Expect(planPtr.Arguments()).To(HaveKeyWithValue("x", 1))
				})

				It("should contain y = string", func() {
					Expect(planPtr.Arguments()).To(HaveKeyWithValue("y", "string"))
				})

				It("should contain z = 0.15", func() {
					Expect(planPtr.Arguments()).To(HaveKeyWithValue("z", 0.15))
				})
			})

			Context("Identifiers", func() {
				It("should have 2 items", func() {
					Expect(planPtr.Identifiers()).To(HaveLen(2))
				})

				It("should contain id1", func() {
					Expect(planPtr.Identifiers()).To(ContainElement("id1"))
				})

				It("should contain id2", func() {
					Expect(planPtr.Identifiers()).To(ContainElement("id2"))
				})
			})

			It("should not have any children", func() {
				Expect(planPtr.Children()).To(BeEmpty())
			})
		})

		When("given map contains a plan with children", func() {
			dict := map[string]interface{}{
				"operatorType": "XOperator",
				"args": map[string]interface{}{
					"x": 1,
					"y": "string",
					"z": 0.15,
				},
				"identifiers": []interface{}{"id1", "id2"},
				"children": []interface{}{
					map[string]interface{}{
						"operatorType": "YOperator",
						"args": map[string]interface{}{
							"k": 2,
						},
						"identifiers": []interface{}{"id3"},
					},
					map[string]interface{}{
						"operatorType": "ZOperator",
						"args": map[string]interface{}{
							"l": 1.35,
						},
					},
				},
			}

			plan := collectPlan(&dict)

			It("should not be nil", func() {
				Expect(plan).NotTo(BeNil())
			})

			It("should have Operator = XOperator", func() {
				Expect(plan.Operator()).To(BeIdenticalTo("XOperator"))
			})

			Context("Args", func() {
				It("should have 3 items", func() {
					Expect(plan.Arguments()).To(HaveLen(3))
				})

				It("should contain x = 1", func() {
					Expect(plan.Arguments()).To(HaveKeyWithValue("x", 1))
				})

				It("should contain y = string", func() {
					Expect(plan.Arguments()).To(HaveKeyWithValue("y", "string"))
				})

				It("should contain z = 0.15", func() {
					Expect(plan.Arguments()).To(HaveKeyWithValue("z", 0.15))
				})
			})

			Context("Identifiers", func() {
				It("should have 2 items", func() {
					Expect(plan.Identifiers()).To(HaveLen(2))
				})

				It("should contain id1", func() {
					Expect(plan.Identifiers()).To(ContainElement("id1"))
				})

				It("should contain id2", func() {
					Expect(plan.Identifiers()).To(ContainElement("id2"))
				})
			})

			Context("Children", func() {
				It("should have 2 children", func() {
					Expect(plan.Children()).To(HaveLen(2))
				})

				Context("Item 0", func() {
					elem := plan.Children()[0]

					It("should have Operator = YOperator", func() {
						Expect(elem.Operator()).To(BeIdenticalTo("YOperator"))
					})

					Context("Args", func() {
						It("should have 1 item", func() {
							Expect(elem.Arguments()).To(HaveLen(1))
						})

						It("should contain k = 2", func() {
							Expect(elem.Arguments()).To(HaveKeyWithValue("k", 2))
						})
					})

					Context("Identifiers", func() {
						It("should have 1 identifier", func() {
							Expect(elem.Identifiers()).To(HaveLen(1))
						})

						It("should contain id3", func() {
							Expect(elem.Identifiers()).To(ContainElement("id3"))
						})
					})

					It("should not have any children", func() {
						Expect(elem.Children()).To(BeEmpty())
					})
				})

				Context("Item 1", func() {
					elem := plan.Children()[1]

					It("should have Operator = ZOperator", func() {
						Expect(elem.Operator()).To(BeIdenticalTo("ZOperator"))
					})

					Context("Args", func() {
						It("should have 1 item", func() {
							Expect(elem.Arguments()).To(HaveLen(1))
						})

						It("should contain l = 1.35", func() {
							Expect(elem.Arguments()).To(HaveKeyWithValue("l", 1.35))
						})
					})

					It("should not have any Identifiers", func() {
						Expect(elem.Identifiers()).To(BeEmpty())
					})

					It("should not have any Children", func() {
						Expect(elem.Children()).To(BeEmpty())
					})
				})
			})
		})
	})

	Context("collectProfiledPlan", func() {
		When("given map is nil", func() {
			It("should not collect anything", func() {
				plan := collectProfile(nil)
				Expect(plan).To(BeNil())
			})
		})

		When("given map contains a plan without children", func() {
			dict := map[string]interface{}{
				"operatorType": "XOperator",
				"rows":         50,
				"dbHits":       1000,
				"args": map[string]interface{}{
					"x": 1,
					"y": "string",
					"z": 0.15,
				},
				"identifiers": []interface{}{"id1", "id2"},
			}

			planPtr := collectProfile(&dict)

			It("should not be nil", func() {
				Expect(planPtr).NotTo(BeNil())
			})

			It("should have Operator = XOperator", func() {
				Expect(planPtr.Operator()).To(BeIdenticalTo("XOperator"))
			})

			It("should have Records = 50", func() {
				Expect(planPtr.Records()).To(BeNumerically("==", 50))
			})

			It("should have DbHits = 1000", func() {
				Expect(planPtr.DbHits()).To(BeNumerically("==", 1000))
			})

			Context("Args", func() {
				It("should have 3 items", func() {
					Expect(planPtr.Arguments()).To(HaveLen(3))
				})

				It("should contain x = 1", func() {
					Expect(planPtr.Arguments()).To(HaveKeyWithValue("x", 1))
				})

				It("should contain y = string", func() {
					Expect(planPtr.Arguments()).To(HaveKeyWithValue("y", "string"))
				})

				It("should contain z = 0.15", func() {
					Expect(planPtr.Arguments()).To(HaveKeyWithValue("z", 0.15))
				})
			})

			Context("Identifiers", func() {
				It("should have 2 items", func() {
					Expect(planPtr.Identifiers()).To(HaveLen(2))
				})

				It("should contain id1", func() {
					Expect(planPtr.Identifiers()).To(ContainElement("id1"))
				})

				It("should contain id2", func() {
					Expect(planPtr.Identifiers()).To(ContainElement("id2"))
				})
			})

			It("should not have any Children", func() {
				Expect(planPtr.Children()).To(BeEmpty())
			})
		})

		When("given map contains a plan with children", func() {
			dict := map[string]interface{}{
				"operatorType": "XOperator",
				"rows":         50,
				"dbHits":       5000,
				"args": map[string]interface{}{
					"x": 1,
					"y": "string",
					"z": 0.15,
				},
				"identifiers": []interface{}{"id1", "id2"},
				"children": []interface{}{
					map[string]interface{}{
						"operatorType": "YOperator",
						"rows":         5,
						"dbHits":       10,
						"args": map[string]interface{}{
							"k": 2,
						},
						"identifiers": []interface{}{"id3"},
					},
					map[string]interface{}{
						"operatorType": "ZOperator",
						"rows":         10,
						"dbHits":       100,
						"args": map[string]interface{}{
							"l": 1.35,
						},
					},
				},
			}

			plan := collectProfile(&dict)

			It("should not be nil", func() {
				Expect(plan).NotTo(BeNil())
			})

			It("should have Operator = XOperator", func() {
				Expect(plan.Operator()).To(BeIdenticalTo("XOperator"))
			})

			It("should have Records = 50", func() {
				Expect(plan.Records()).To(BeNumerically("==", 50))
			})

			It("should have DbHits = 5000", func() {
				Expect(plan.DbHits()).To(BeNumerically("==", 5000))
			})

			Context("Args", func() {
				It("should have 3 items", func() {
					Expect(plan.Arguments()).To(HaveLen(3))
				})

				It("should contain x = 1", func() {
					Expect(plan.Arguments()).To(HaveKeyWithValue("x", 1))
				})

				It("should contain y = string", func() {
					Expect(plan.Arguments()).To(HaveKeyWithValue("y", "string"))
				})

				It("should contain z = 0.15", func() {
					Expect(plan.Arguments()).To(HaveKeyWithValue("z", 0.15))
				})
			})

			Context("Identifiers", func() {
				It("should have 2 items", func() {
					Expect(plan.Identifiers()).To(HaveLen(2))
				})

				It("should contain id1", func() {
					Expect(plan.Identifiers()).To(ContainElement("id1"))
				})

				It("should contain id2", func() {
					Expect(plan.Identifiers()).To(ContainElement("id2"))
				})
			})

			Context("Children", func() {
				It("should have 2 children", func() {
					Expect(plan.Children()).To(HaveLen(2))
				})

				Context("Item 0", func() {
					elem := plan.Children()[0]

					It("should have Operator = YOperator", func() {
						Expect(elem.Operator()).To(BeIdenticalTo("YOperator"))
					})

					It("should have Records = 5", func() {
						Expect(elem.Records()).To(BeNumerically("==", 5))
					})

					It("should have DbHits = 10", func() {
						Expect(elem.DbHits()).To(BeNumerically("==", 10))
					})

					Context("Args", func() {
						It("should have 1 item", func() {
							Expect(elem.Arguments()).To(HaveLen(1))
						})

						It("should contain k = 2", func() {
							Expect(elem.Arguments()).To(HaveKeyWithValue("k", 2))
						})
					})

					Context("Identifiers", func() {
						It("should have 1 identifier", func() {
							Expect(elem.Identifiers()).To(HaveLen(1))
						})

						It("should contain id3", func() {
							Expect(elem.Identifiers()).To(ContainElement("id3"))
						})
					})

					It("should not have any Children", func() {
						Expect(elem.Children()).To(BeEmpty())
					})
				})

				Context("Item 1", func() {
					elem := plan.Children()[1]

					It("should have Operator = ZOperator", func() {
						Expect(elem.Operator()).To(BeIdenticalTo("ZOperator"))
					})

					It("should have Records = 5", func() {
						Expect(elem.Records()).To(BeNumerically("==", 10))
					})

					It("should have DbHits = 10", func() {
						Expect(elem.DbHits()).To(BeNumerically("==", 100))
					})

					Context("Args", func() {
						It("should have 1 item", func() {
							Expect(elem.Arguments()).To(HaveLen(1))
						})

						It("should contain l = 1.35", func() {
							Expect(elem.Arguments()).To(HaveKeyWithValue("l", 1.35))
						})
					})

					It("should not have any Identifiers", func() {
						Expect(elem.Identifiers()).To(BeEmpty())
					})

					It("should not have any Children", func() {
						Expect(elem.Children()).To(BeEmpty())
					})
				})
			})
		})
	})
})
