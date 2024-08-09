from multiprocessing import Queue

# This queue will be inherited by child processes
# Used to communicate with the main process
part_metadata_queue = Queue()
