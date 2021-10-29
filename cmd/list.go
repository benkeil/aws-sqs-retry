/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

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
package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spf13/cobra"
)

type Queue struct {
	Url                         string
	Name                        string
	ApproximateNumberOfMessages int
}

type ByApproximateNumberOfMessages []Queue

func (a ByApproximateNumberOfMessages) Len() int { return len(a) }
func (a ByApproximateNumberOfMessages) Less(i, j int) bool {
	return a[i].ApproximateNumberOfMessages < a[j].ApproximateNumberOfMessages
}
func (a ByApproximateNumberOfMessages) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		sess := session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		}))
		service := sqs.New(sess)

		wgReceivers := sync.WaitGroup{}

		res, err := service.ListQueues(&sqs.ListQueuesInput{})
		if err != nil {
			return err
		}

		queues := []Queue{}

		for _, url := range res.QueueUrls {
			if strings.HasSuffix(*url, "dlq") || strings.HasSuffix(*url, "dlq.fifo") {
				wgReceivers.Add(1)
				go func(url *string) error {
					defer wgReceivers.Done()
					atr, err := service.GetQueueAttributes(&sqs.GetQueueAttributesInput{
						QueueUrl:       url,
						AttributeNames: aws.StringSlice([]string{"ApproximateNumberOfMessages"}),
					})
					if err != nil {
						return err
					}
					messages := *atr.Attributes["ApproximateNumberOfMessages"]
					count, _ := strconv.Atoi(messages)
					name := (*url)[strings.LastIndex(*url, "/")+1:]
					if count > 0 {
						queues = append(queues, Queue{Url: *url, Name: name, ApproximateNumberOfMessages: count})
					}
					return nil
				}(url)
			}
		}

		wgReceivers.Wait()
		sort.Sort(ByApproximateNumberOfMessages(queues))
		max := findMax(queues)
		l := strconv.Itoa(len(strconv.Itoa(max)))
		format := "%0" + l + "d %s"
		for _, queue := range queues {
			fmt.Println(fmt.Sprintf(format, queue.ApproximateNumberOfMessages, queue.Name))
		}

		return nil
	},
}

func findMax(queues []Queue) int {
	max := 0
	for _, queue := range queues {
		if queue.ApproximateNumberOfMessages > max {
			max = queue.ApproximateNumberOfMessages
		}
	}
	return max
}

func init() {
	rootCmd.AddCommand(listCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// listCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// listCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
