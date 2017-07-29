using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WorkerPool {

    /// <summary>
    /// A very light-weight, simple worker pool utility class for running tasks in a queue model.
    /// Auto-scaling is automatic, where additional worker threads are added as the queue grows,
    /// or removed as the queue size decreases.
    /// This class is thread-safe and can be used in a multi-threaded environment.
    /// </summary>
    public class WorkerPool {


        /// <summary>
        /// A single work task which can be processed by the Worker Pool Thread.
        /// This abstract class should be implmented to accomplish the work task.
        /// A worker pool can process different types of tasks.
        /// </summary>
        public abstract class WorkerTask {

            public WorkerThread AssignedWorkerThread = null;

            /// <summary>
            /// Processes the work task. Implmented this method to accomplish the work task.
            /// </summary>
            public abstract void Process();

        }

        /// <summary>
        /// A Worker Thread which belongs to a Worker Pool.
        /// </summary>
        public class WorkerThread {

            private string _id = null;
            private WorkerPool _pool = null;
            private Thread _thread = null;
            private bool _requestStop = false;
            private bool _isProcessingTask = false;

            private DateTime _taskStartTime;
            private DateTime _taskEndTime;

            public WorkerThread(WorkerPool pool) {
                _id = Guid.NewGuid().ToString();
                _pool = pool;
                _thread = new Thread(_doWork);
            }

            public bool IsProcessingTask() {
                return _isProcessingTask;
            }

            /// <summary>
            /// Returns the current task process time in milliseconds.
            /// If no task is running, 0 is returned.
            /// </summary>
            /// <returns></returns>
            public long CurrentTaskProcessTime() {
                if (_isProcessingTask == false) return 0;
                else return (long)((DateTime.UtcNow - _taskStartTime).TotalMilliseconds);
            }

            public void Start() {
                _requestStop = false;
                _thread.Start();
            }

            public string Id() {
                return _id;
            }

            public void Stop() {
                _requestStop = true;
            }

            public void Abort() {
                _thread.Abort(); 
            }

            private void _doWork() {
                while (!_requestStop) {
                    WorkerTask task = _pool.DequeueTask();
                    if (task != null) {
                        // Process the task
                        task.AssignedWorkerThread = this;
                        try {
                            _isProcessingTask = true;
                            _taskStartTime = DateTime.UtcNow;

                            task.Process();

                            _isProcessingTask = false;
                            _taskEndTime = DateTime.UtcNow;
                        } catch (Exception e) {
                            _isProcessingTask = false;
                            _taskEndTime = DateTime.UtcNow;
                        }
                    } else {
                        // Wait a bit
                        Thread.Sleep(10);
                    }
                }
                // Finish
                _pool.RemoveWorker(this);
            }
        }



        /// <summary>
        /// Interface for providing a log sink to see what is going on in the worker pool.
        /// </summary>
        public interface IWorkerPoolLogSink {
            void Log(string message);
        }

        private ConcurrentQueue<WorkerTask> _queue = new ConcurrentQueue<WorkerTask>();
        private List<WorkerThread> _workers = new List<WorkerThread>();
        private object _workersLock = new object();
        private IWorkerPoolLogSink _logSink = null;
        private DateTime _lastScale = new DateTime(0);

        public int MinWorkerThreads                     = 1;
        public int MaxWorkerThreads                     = 20;
        public int AutoScalingQueueScaleUpThreshold     = 50;
        public int AutoScalingQueueScaleDownThreshold   = 20;
        public int AutoScalingScaleUpIncrement          = 1;
        public int AutoScalingScaleDownIncrement        = 1;
        public TimeSpan AutoScalingScaleUpCooldown      = new TimeSpan(0, 0, 30); // h,m,s
        public TimeSpan AutoScalingScaleDownCooldown = new TimeSpan(0, 0, 30); // h,m,s
        public int WorkerThreadTaskProcessTimeoutSeconds = 60;

        public void SetLogSink(IWorkerPoolLogSink sink) {
            _logSink = sink;
        }

        protected void _log(string message) {
            if (_logSink != null) {
                _logSink.Log(message);
            }
        }

        private bool _performAutoScale() {
            // Make sure at least one worker is around
            if (_workers.Count == 0) {
                _log("Auto-Scaling up due to no workers");
                _scale(+1);
                return true;
            }
            // Time to scale?
            bool didScale = false;
            if (_lastScale < DateTime.Now.Subtract(AutoScalingScaleUpCooldown) 
                && _queue.Count > AutoScalingQueueScaleUpThreshold) {
                // Scale up
                    _log("Auto-Scaling up due to large queue (" + _queue.Count + ")");
                _lastScale = DateTime.Now;
                _scale(AutoScalingScaleUpIncrement);
                didScale = true;
            } else if (_workers.Count > 1
                && _lastScale < DateTime.Now.Subtract(AutoScalingScaleDownCooldown)
                && _queue.Count < AutoScalingQueueScaleDownThreshold) {
                // Scale down
                    _log("Auto-Scaling down due to small queue (" + _queue.Count + ")");
                _lastScale = DateTime.Now;
                _scale(-AutoScalingScaleDownIncrement);
                didScale = true;
            }
            return didScale;
        }

        public void QueueTask(WorkerTask task) {
            _log("Queue size " + _queue.Count + ", workers " + _workers.Count);
            _queue.Enqueue(task);
            lock (_workersLock) {
                _checkForLockedWorkerThreads();
                _performAutoScale();
            }
        }

        public void RemoveWorker(WorkerThread worker) {
            lock (_workersLock) {
                _log("Removing worker " + worker.Id());
                _workers.Remove(worker);
            }
        }

        public WorkerTask DequeueTask() {
            _log("Queue size " + _queue.Count + ", workers " + _workers.Count);
            WorkerTask task;
            _queue.TryDequeue(out task);
            return task;
        }

        /// <summary>
        /// Checks for locked worker threads and removes any which are locked.
        /// A worker thread is considered locked if a task does not finish within
        /// WorkerThreadTaskProcessTimeoutSeconds seconds.
        /// </summary>
        private void _checkForLockedWorkerThreads() {
            for (int i = 0; i < _workers.Count; i++) {
                WorkerThread worker = null;
                try {
                    worker = _workers.ElementAt(i);
                } catch (Exception e) { }
                if (worker == null) break;
                if (worker.CurrentTaskProcessTime() > WorkerThreadTaskProcessTimeoutSeconds * 1000) {
                    _log("Warning: worker thread is locked");
                    _workers.Remove(worker);
                    worker.Abort();
                }
            }
        }

        private void _scale(int count) {
            _log("Scaling " + count);
            if (count > 0) {
                for (int i = 0; i < count; i++) {
                    if (_workers.Count >= MaxWorkerThreads) return;
                    WorkerThread worker = new WorkerThread(this);
                    _workers.Add(worker);
                    _log("Starting worker " + worker.Id());
                    worker.Start();
                }
            } else {
                for (int i = 0; i < -count; i++) {
                    if (_workers.Count <= MinWorkerThreads) return;
                    WorkerThread worker = null;
                    try {
                        worker = _workers.ElementAt(i);
                    } catch (Exception e) { }
                    if (worker == null) break;
                    _log("Stopping worker " + worker.Id());
                    worker.Stop();
                }
            }
        }

        public void Scale(int count) {
            lock (_workersLock) {
                _scale(count);
            }
        }

        public void ScaleUp() {
            Scale(1);
        }
        public void ScaleDown() {
            Scale(-1);
        }

        public void ProcessQueue() {
            while (_queue.Count() > 0) {
                Thread.Sleep(1);
            }
        }

        public int QueueSize() {
            return _queue.Count();
        }

        public int PoolSize() {
            return _workers.Count();
        }

        public void Shutdown(bool wait = true) {
            for (int i = 0; i < _workers.Count; i++) {
                WorkerThread worker = null;
                try {
                    worker = _workers.ElementAt(i);
                } catch (Exception e) { }
                if (worker == null) break;
                _log("Stopping worker " + worker.Id() + " due to shutdown");
                worker.Stop();
            }
            if (wait) {
                while (_workers.Count() > 0) {
                    Thread.Sleep(1);
                }
            }
        }

    }
}

