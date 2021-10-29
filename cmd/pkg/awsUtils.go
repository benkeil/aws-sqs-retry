package utils

import "github.com/aws/aws-sdk-go/aws/session"

func NewAwsSession() (*session.Session, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	return sess, nil
}
