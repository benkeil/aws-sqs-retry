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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spf13/cobra"
)

var DlqName string
var QueueName string

// localCmd represents the local command
var localCmd = &cobra.Command{
	Use:   "local",
	Short: "retry messages on the executing machine",
	Long:  `Messages get retried localy on the executin machine.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// @TODO move to a package
		sess := session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		}))
		service := sqs.New(sess)

		// @TODO move to a package
		dlqRrlResult, err := service.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: &DlqName,
		})
		if err != nil {
			return err
		}
		dlqQueueURL := dlqRrlResult.QueueUrl

		a, _ := service.ListDeadLetterSourceQueues(&sqs.ListDeadLetterSourceQueuesInput{})
		fmt.Println(fmt.Sprintf("%v", a))

		queueRrlResult, err := service.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: &QueueName,
		})
		if err != nil {
			return err
		}
		queueURL := queueRrlResult.QueueUrl

		fmt.Println(fmt.Sprintf("sending messages from %s to %s", *dlqQueueURL, *queueURL))

		messageChannel := make(chan *sqs.Message, 10)

		go pollMessages(service, dlqQueueURL, messageChannel)

		for message := range messageChannel {
			go forwardMessage(service, dlqQueueURL, queueURL, message)
		}

		return nil
	},
}

func pollMessages(service *sqs.SQS, dlqQueueURL *string, messageChannel chan<- *sqs.Message) {
	for {
		res, err := service.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(*dlqQueueURL),
			MaxNumberOfMessages:   aws.Int64(1),
			WaitTimeSeconds:       aws.Int64(20),
			MessageAttributeNames: aws.StringSlice([]string{"All"}),
		})
		if err != nil {
			fmt.Println(fmt.Sprintf("failed to fetch sqs message %v", err))
		}
		for _, message := range res.Messages {
			messageChannel <- message
		}
	}
}

func forwardMessage(service *sqs.SQS, dlqQueueURL *string, queueURL *string, message *sqs.Message) {
	fmt.Println(fmt.Sprintf("forward: %v", message))
	service.SendMessage(&sqs.SendMessageInput{
		MessageAttributes: message.MessageAttributes,
		MessageBody:       message.Body,
		QueueUrl:          queueURL,
	})
	service.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      dlqQueueURL,
		ReceiptHandle: message.ReceiptHandle,
	})
}

func init() {
	rootCmd.AddCommand(localCmd)
	localCmd.Flags().StringVarP(&DlqName, "dlq", "d", "", "The dead letter queue")
	localCmd.Flags().StringVarP(&QueueName, "queue", "q", "", "The target queue")
	localCmd.MarkFlagRequired("dlq")
	localCmd.MarkFlagRequired("queue")
}
