import logging
from threading import Thread, Event
from queue import Queue

class WorkerPool:
    def __init__(self, worker_count, max_queue_size=100):
        self.worker_count = worker_count
        self.job_queue = Queue(maxsize=max_queue_size)
        self.workers = []
        self.stop_event = Event()

    def start(self):
        for worker_id in range(1, self.worker_count + 1):
            worker = Thread(target=self._worker_task, args=(worker_id,))
            worker.daemon = True
            self.workers.append(worker)
            worker.start()
        logging.info(f"WorkerPool started with {self.worker_count} workers.")

    def stop(self):
        self.stop_event.set()
        for worker in self.workers:
            worker.join()
        logging.info("WorkerPool stopped.")

    def add_job(self, job):
        self.job_queue.put(job, block=False)

    def _worker_task(self, worker_id):
        logging.info(f"Worker {worker_id} started.")
        while not self.stop_event.is_set():
            try:
                job = self.job_queue.get(timeout=1)
                self.process_job(worker_id, job)
                self.job_queue.task_done()
            except:
                continue

    def process_job(self, worker_id, job):
        # logging.info(f"Worker {worker_id} processing job: {job}")
        logging.info(f"Worker {worker_id} processing job.")
    

