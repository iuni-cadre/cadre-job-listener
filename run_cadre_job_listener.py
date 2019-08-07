from multiprocessing import Process
import backend.job_listener
import backend.package_listener

if __name__ == '__main__':
    job_process = Process(target=backend.job_listener.poll_queue())
    job_process.start()

    package_process = Process(target=backend.package_listener.poll_queue())
    package_process.start()
