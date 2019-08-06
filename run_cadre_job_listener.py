import backend.job_listener
import backend.package_listener

if __name__ == '__main__':
    backend.job_listener.poll_queue()
    backend.package_listener.poll_queue()
