import backend.sqs_listener

if __name__ == '__main__':
    backend.sqs_listener.poll_queue()
