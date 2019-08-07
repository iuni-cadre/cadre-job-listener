import threading
import backend.job_listener
import backend.package_listener

if __name__ == '__main__':
    thread1 = threading.Thread(target=backend.job_listener.poll_queue)
    thread1.start()

    thread2 = threading.Thread(target=backend.package_listener.poll_queue)
    thread2.start()

