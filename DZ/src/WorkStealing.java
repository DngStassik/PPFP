import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class WorkStealing {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    enum TaskMode {
        BLACK_HOLE,
        SLEEP
    }

    interface Shutdownable{
        void shutdown();
    }

    public interface ShutdownableExecutor extends Shutdownable, Executor{}
    static class ThreadPerTaskExecutor implements ShutdownableExecutor{
        List<Thread> threads = new ArrayList<>();
        @Override
        public void execute(Runnable command) {
            var t = new Thread(command);
            threads.add(t);
            t.start();
        }
        public void shutdown(){
            threads.forEach(t -> {try {t.join();} catch (InterruptedException e) {throw new RuntimeException(e);}});
        }
    }

    static class FixedThreadPoolExecutor implements ShutdownableExecutor {
        private final ExecutorService executor;

        FixedThreadPoolExecutor(int threads) {
            executor = Executors.newFixedThreadPool(threads);
        }

        @Override
        public void execute(Runnable command) {
            executor.execute(command);
        }

        @Override
        public void shutdown() {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS))
                    throw new IllegalStateException("Executor did not terminate");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {

        protected static final Runnable EXIT_TASK = () -> {};

        AtomicInteger counter = new AtomicInteger(0);

        List<Thread> threads;
        Vector<BlockingDeque<Runnable>> tasks;
        volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threads){
            Supplier<IntStream> stream = () -> IntStream.iterate(0, x->x<threads, x->x+1);
            tasks = new Vector<>(
                    stream.get()
                            .mapToObj(x-> new LinkedBlockingDeque<Runnable>())
                            .toList());
            this.threads = stream.get()
                    .mapToObj(this::spawnThread)
                    .toList();
            this.threads.forEach(Thread::start);
        }

        Thread spawnThread(int id){
            return new Thread(() ->
            {
                while (true) {
                    Runnable task;
                    try {
                        task = tasks.get(id).takeFirst();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (task == EXIT_TASK)
                        return;
                    task.run();
                }
            }
            );
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    tasks.forEach(queue -> queue.addLast(EXIT_TASK));
                }
            }
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown)
                throw new RejectedExecutionException("Executor is shutting down");
            tasks.get(counter.addAndGet(1) % threads.size()).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {

        WorkStealingExecutor(int threads) {
            super(threads);
        }

        @Override
        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task = tasks.get(id).pollFirst();
                    if (task == null)
                        task = stealTask(id);

                    if (task == null) {
                        try {
                            task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == null)
                            continue;
                    }

                    if (task == EXIT_TASK) {
                        task = stealTask(id);
                        if (task == null)
                            return;
                        tasks.get(id).addLast(EXIT_TASK);
                    }

                    task.run();
                }
            });
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                var queue = tasks.get((thiefId + offset) % tasks.size());
                var task = queue.pollLast();

                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }

                if (task != null)
                    return task;
            }
            return null;
        }
    }

    // Это вещи, которые нам нужны
    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 100_000;
    static final long TARGET_TOTAL_WORK = 100_000_000L;

    // Это всякое вспомогательное побочное
    static final long MEAN_TASK_WORK = TARGET_TOTAL_WORK / TASK_NUMBER;
    static final long LOWER_TASK_WORK_BOUND = 0;
    static final long HIGHER_TASK_WORK_BOUND = MEAN_TASK_WORK * 2 + 1;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final long PARETO_RAW_MAX_TASK_WORK = MEAN_TASK_WORK * 100;
    static volatile long blackHoleResult;

    static long blackHole(long difficulty) {
        long value = 1;
        for (long i = 0; i < difficulty; i++)
            value = value * 31 + i;
        return value;
    }

    static Runnable createTask(long difficulty, TaskMode mode) {
        return switch (mode) {
            case BLACK_HOLE -> new BlackHoleTask(difficulty);
            case SLEEP -> new SleepTask(difficulty);
        };
    }

    static final class BlackHoleTask implements Runnable {
        private final long difficulty;

        BlackHoleTask(long difficulty) {
            this.difficulty = difficulty;
        }

        @Override
        public void run() {
            blackHoleResult = blackHole(difficulty);
        }
    }

    static final class SleepTask implements Runnable {
        private final long difficulty;

        SleepTask(long difficulty) {
            this.difficulty = difficulty;
        }

        @Override
        public void run() {
            long sleepNanos = difficulty * 1_000_000L / MEAN_TASK_WORK;
            try {
                Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Sleep task was interrupted", e);
            }
        }
    }

    static long createTaskWork(TaskDistribution distribution, int taskId, Random random) {
        return switch (distribution) {
            case UNIFORM -> random.nextLong(LOWER_TASK_WORK_BOUND, HIGHER_TASK_WORK_BOUND);
            case PERIODIC -> taskId % THREAD_NUMBER == 0
                    ? MEAN_TASK_WORK * THREAD_NUMBER
                    : 0;
            case PARETO -> {
                var scale = MEAN_TASK_WORK * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                var work = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                yield Math.min(Math.round(work), PARETO_RAW_MAX_TASK_WORK);
            }
        };
    }

    static long[] createTaskWorkloads(TaskDistribution distribution) {
        var random = new Random(RANDOM_SEED);
        var workloads = new long[TASK_NUMBER];

        long totalWork = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            workloads[taskId] = createTaskWork(distribution, taskId, random);
            totalWork += workloads[taskId];
        }

        double scale = (double) TARGET_TOTAL_WORK / totalWork;
        double remainder = 0;
        long normalizedWork = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledWork = workloads[taskId] * scale + remainder;
            workloads[taskId] = (long) scaledWork;
            remainder = scaledWork - workloads[taskId];
            normalizedWork += workloads[taskId];
        }

        for (int taskId = 0; normalizedWork < TARGET_TOTAL_WORK; taskId++) {
            workloads[taskId % TASK_NUMBER]++;
            normalizedWork++;
        }
        for (int taskId = 0; normalizedWork > TARGET_TOTAL_WORK; taskId++) {
            int index = taskId % TASK_NUMBER;
            if (workloads[index] > 0) {
                workloads[index]--;
                normalizedWork--;
            }
        }

        return workloads;
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution, TaskMode mode) {
        var workloads = createTaskWorkloads(distribution);
        return new Vector<>(
                IntStream
                        .iterate(0, x -> x < TASK_NUMBER, x -> x + 1)
                        .mapToObj(x -> createTask(workloads[x], mode))
                        .toList()
        );
    }

    public record Pair<A, B>(A first, B second) {}

    public static Pair<Long, Long> measureExecutor(ShutdownableExecutor executor,
                                                    TaskDistribution distribution, TaskMode mode) {
        var tasks = createTasks(distribution, mode);
        var start = System.nanoTime();
        tasks.forEach(executor::execute);
        var submissionEnd = System.nanoTime();
        executor.shutdown();
        var finish = System.nanoTime();
        return new Pair<>(submissionEnd - start, finish - start);
    }

    public static void main(String[] args) {
        System.out.println("Tasks: " + TASK_NUMBER + ", total work: " + TARGET_TOTAL_WORK
                + ", worker threads: " + THREAD_NUMBER);
        System.out.println("SLEEP uses 1 ms per " + MEAN_TASK_WORK + " work units.");

        for (var distribution : TaskDistribution.values()) {
            System.out.println("Task distribution: " + distribution);
            printComparison("Thread per task", distribution, ThreadPerTaskExecutor::new);
            printComparison("Fixed thread pool", distribution,
                    () -> new FixedThreadPoolExecutor(THREAD_NUMBER));
            printComparison("Round robin", distribution,
                    () -> new RoundRobinExecutor(THREAD_NUMBER));
            printComparison("Work stealing", distribution,
                    () -> new WorkStealingExecutor(THREAD_NUMBER));
        }
    }

    static void printComparison(String name, TaskDistribution distribution,
                                Supplier<ShutdownableExecutor> executorFactory) {
        var blackHoleResult = measureExecutor(executorFactory.get(), distribution, TaskMode.BLACK_HOLE);
        var sleepResult = measureExecutor(executorFactory.get(), distribution, TaskMode.SLEEP);
        printResult(name, blackHoleResult, sleepResult);
    }

    static void printResult(String name, Pair<Long, Long> blackHoleResult, Pair<Long, Long> sleepResult) {
        System.out.printf(Locale.ROOT, "  %-20s blackHole: %8.3f ms, sleep: %8.3f ms%n",
                name, blackHoleResult.second / 1_000_000d, sleepResult.second / 1_000_000d);
    }
}