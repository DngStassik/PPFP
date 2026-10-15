import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз;
 * - для каждого URL requests = successful + errors;
 * - maxDuration <= totalDuration при наличии запросов;
 * - 0 <= activeFiles <= semaphoreLimit;
 * - после завершения анализа activeFiles равно нулю;
 * - один повреждённый файл не мешает обработке остальных;
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */
public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis,
            Map<String, PathStatistics> paths,
            long filesProcessed,
            long linesRead,
            long validRecords,
            long malformedRecords,
            long readErrors,
            int activeFiles,
            int maxActiveFiles,
            boolean stopped,
            List<String> failedFiles
    ) {
        public Report {
            paths = Collections.unmodifiableMap(new TreeMap<>(paths));
            failedFiles = List.copyOf(failedFiles);
        }
    }

    public enum LockingStrategy {
        GLOBAL,
        PER_PATH
    }

    public static final class LogAnalyzer {
        private final int workerCount;
        private final Semaphore fileSemaphore;
        private final LockingStrategy lockingStrategy;
        private final Statistics statistics;
        private final AtomicLong filesProcessed = new AtomicLong();
        private final AtomicLong linesRead = new AtomicLong();
        private final AtomicLong validRecords = new AtomicLong();
        private final AtomicLong malformedRecords = new AtomicLong();
        private final AtomicLong readErrors = new AtomicLong();
        private final AtomicInteger activeFiles = new AtomicInteger();
        private final AtomicInteger maxActiveFiles = new AtomicInteger();
        private final ConcurrentLinkedQueue<String> failedFiles =
                new ConcurrentLinkedQueue<>();
        private volatile boolean stopRequested;

        public LogAnalyzer(
                int workerCount,
                int semaphoreLimit,
                LockingStrategy lockingStrategy
        ) {
            if (workerCount < 1 || semaphoreLimit < 1) {
                throw new IllegalArgumentException(
                        "Worker count and semaphore limit must be positive"
                );
            }
            if (lockingStrategy == null) {
                throw new IllegalArgumentException(
                        "Locking strategy must not be null"
                );
            }

            this.workerCount = workerCount;
            this.fileSemaphore = new Semaphore(semaphoreLimit, true);
            this.lockingStrategy = lockingStrategy;
            this.statistics = new Statistics(lockingStrategy);
        }

        public void stop() {
            stopRequested = true;
        }

        public Report analyze(List<LogFile> files) throws InterruptedException {
            if (files == null) {
                throw new IllegalArgumentException("Files must not be null");
            }
            for (LogFile file : files) {
                if (file == null || file.path() == null) {
                    throw new IllegalArgumentException(
                            "Log files and their paths must not be null"
                    );
                }
            }

            ExecutorService executor = Executors.newFixedThreadPool(workerCount);
            CountDownLatch finishedFiles = new CountDownLatch(files.size());

            try {
                for (LogFile file : files) {
                    executor.execute(() -> {
                        try {
                            processFile(file);
                        } finally {
                            finishedFiles.countDown();
                        }
                    });
                }

                finishedFiles.await();
            } finally {
                executor.shutdown();
                if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                        throw new IllegalStateException(
                                "Worker threads did not stop"
                        );
                    }
                }
            }

            return createReport();
        }

        private void processFile(LogFile logFile) {
            boolean permitAcquired = false;
            boolean fileIsActive = false;

            try {
                if (stopRequested) {
                    return;
                }

                fileSemaphore.acquire();
                permitAcquired = true;

                if (stopRequested) {
                    return;
                }

                int currentActive = activeFiles.incrementAndGet();
                fileIsActive = true;
                updateMaximumActiveFiles(currentActive);

                Iterator<String> lines = LogFileLines.iterator(logFile.path());
                boolean reachedEnd = true;

                while (lines.hasNext()) {
                    if (stopRequested) {
                        reachedEnd = false;
                        break;
                    }

                    String line = lines.next();
                    linesRead.incrementAndGet();
                    try {
                        LogEntry entry = parseLine(line);
                        statistics.add(entry);
                        validRecords.incrementAndGet();
                    } catch (IllegalArgumentException exception) {
                        malformedRecords.incrementAndGet();
                    }
                }

                if (reachedEnd) {
                    filesProcessed.incrementAndGet();
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                recordFileError(logFile, "File processing was interrupted");
            } catch (RuntimeException exception) {
                recordFileError(logFile, exception.getMessage());
            } finally {
                if (fileIsActive) {
                    activeFiles.decrementAndGet();
                }
                if (permitAcquired) {
                    fileSemaphore.release();
                }
            }
        }

        private void updateMaximumActiveFiles(int currentActive) {
            int previousMaximum = maxActiveFiles.get();
            while (currentActive > previousMaximum
                    && !maxActiveFiles.compareAndSet(
                            previousMaximum,
                            currentActive
                    )) {
                previousMaximum = maxActiveFiles.get();
            }
        }

        private void recordFileError(LogFile file, String message) {
            readErrors.incrementAndGet();
            failedFiles.add(file.path() + ": "
                    + (message == null ? "Unknown file error" : message));
        }

        private Report createReport() {
            return LogAnalysisAssignment.createReport(
                    statistics,
                    filesProcessed.get(),
                    linesRead.get(),
                    validRecords.get(),
                    malformedRecords.get(),
                    readErrors.get(),
                    activeFiles.get(),
                    maxActiveFiles.get(),
                    stopRequested,
                    new ArrayList<>(failedFiles)
            );
        }
    }

    private static Report createReport(
            Statistics statistics,
            long filesProcessed,
            long linesRead,
            long validRecords,
            long malformedRecords,
            long readErrors,
            int activeFiles,
            int maxActiveFiles,
            boolean stopped,
            List<String> failedFiles
    ) {
        Map<String, PathStatistics> paths = statistics.snapshot();
        long requests = 0;
        long successful = 0;
        long errors = 0;
        long bytes = 0;
        long duration = 0;
        long maximumDuration = 0;

        for (PathStatistics path : paths.values()) {
            requests += path.requests();
            successful += path.successful();
            errors += path.errors();
            bytes += path.responseBytes();
            duration += path.totalDurationMillis();
            maximumDuration = Math.max(
                    maximumDuration,
                    path.maxDurationMillis()
            );
        }

        failedFiles.sort(Comparator.naturalOrder());
        return new Report(
                requests,
                successful,
                errors,
                bytes,
                duration,
                maximumDuration,
                paths,
                filesProcessed,
                linesRead,
                validRecords,
                malformedRecords,
                readErrors,
                activeFiles,
                maxActiveFiles,
                stopped,
                failedFiles
        );
    }

    private static final class Statistics {
        private final LockingStrategy strategy;
        private final Map<String, PathAccumulator> paths =
                new ConcurrentHashMap<>();

        private Statistics(LockingStrategy strategy) {
            this.strategy = strategy;
        }

        private synchronized void addGlobally(LogEntry entry) {
            paths.computeIfAbsent(entry.path(), key -> new PathAccumulator())
                    .add(entry);
        }

        private void addByPath(LogEntry entry) {
            PathAccumulator accumulator = paths.computeIfAbsent(
                    entry.path(),
                    key -> new PathAccumulator()
            );
            accumulator.add(entry);
        }

        private void add(LogEntry entry) {
            if (strategy == LockingStrategy.GLOBAL) {
                addGlobally(entry);
            } else {
                addByPath(entry);
            }
        }

        private Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> result = new TreeMap<>();
            for (Map.Entry<String, PathAccumulator> entry : paths.entrySet()) {
                result.put(entry.getKey(), entry.getValue().snapshot());
            }
            return result;
        }
    }

    private static final class PathAccumulator {
        private long requests;
        private long successful;
        private long errors;
        private long responseBytes;
        private long totalDurationMillis;
        private long maxDurationMillis;

        private synchronized void add(LogEntry entry) {
            requests++;
            if (entry.status() < 400) {
                successful++;
            } else {
                errors++;
            }
            responseBytes += entry.responseBytes();
            totalDurationMillis += entry.durationMillis();
            maxDurationMillis = Math.max(
                    maxDurationMillis,
                    entry.durationMillis()
            );
        }

        private synchronized PathStatistics snapshot() {
            return new PathStatistics(
                    requests,
                    successful,
                    errors,
                    responseBytes,
                    totalDurationMillis,
                    maxDurationMillis
            );
        }
    }

    public static LogEntry parseLine(String line) {
        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException("Log line is empty");
        }

        String[] fields = line.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException(
                    "Expected 5 fields, got " + fields.length
            );
        }

        try {
            int status = Integer.parseInt(fields[2]);
            long responseBytes = Long.parseLong(fields[3]);
            long durationMillis = Long.parseLong(fields[4]);

            if (status < 200) {
                throw new IllegalArgumentException(
                        "Status must be at least 200"
                );
            }
            if (responseBytes < 0 || durationMillis < 0) {
                throw new IllegalArgumentException(
                        "Bytes and duration must be non-negative"
                );
            }

            return new LogEntry(
                    fields[0],
                    fields[1],
                    status,
                    responseBytes,
                    durationMillis
            );
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Status, bytes and duration must be integers",
                    exception
            );
        }
    }

    public static Report analyzeSequential(List<LogFile> files) {
        if (files == null) {
            throw new IllegalArgumentException("Files must not be null");
        }

        Statistics statistics = new Statistics(LockingStrategy.GLOBAL);
        long filesProcessed = 0;
        long linesRead = 0;
        long validRecords = 0;
        long malformedRecords = 0;
        long readErrors = 0;
        List<String> failedFiles = new ArrayList<>();

        for (LogFile file : files) {
            if (file == null || file.path() == null) {
                throw new IllegalArgumentException(
                        "Log files and their paths must not be null"
                );
            }

            try {
                Iterator<String> lines = LogFileLines.iterator(file.path());
                while (lines.hasNext()) {
                    String line = lines.next();
                    linesRead++;
                    try {
                        statistics.add(parseLine(line));
                        validRecords++;
                    } catch (IllegalArgumentException exception) {
                        malformedRecords++;
                    }
                }
                filesProcessed++;
            } catch (RuntimeException exception) {
                readErrors++;
                failedFiles.add(file.path() + ": "
                        + (exception.getMessage() == null
                        ? "Unknown file error"
                        : exception.getMessage()));
            }
        }

        return createReport(
                statistics,
                filesProcessed,
                linesRead,
                validRecords,
                malformedRecords,
                readErrors,
                0,
                files.isEmpty() ? 0 : 1,
                false,
                failedFiles
        );
    }


    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    private void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }
    
    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }

    private static void selfCheck() throws Exception {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Unexpected line: " + line
                        );
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );

        checkAnalysis();
    }

    private static void checkAnalysis() throws Exception {
        Path directory = Files.createTempDirectory("log-analysis-check-");
        Path firstFile = directory.resolve("first.log");
        Path secondFile = directory.resolve("second.log");
        Path missingFile = directory.resolve("missing.log");

        Files.writeString(
                firstFile,
                "GET /ok 200 10 5\n"
                        + "GET /bad 500 20 8\n"
                        + "not a valid record\n",
                StandardCharsets.UTF_8
        );
        Files.writeString(
                secondFile,
                "POST /ok 201 30 2\n",
                StandardCharsets.UTF_8
        );

        List<LogFile> files = List.of(
                new LogFile(firstFile),
                new LogFile(secondFile),
                new LogFile(missingFile)
        );
        Report globalReport = new LogAnalyzer(
                4,
                1,
                LockingStrategy.GLOBAL
        ).analyze(files);
        Report perPathReport = new LogAnalyzer(
                4,
                1,
                LockingStrategy.PER_PATH
        ).analyze(files);
        Report sequentialReport = analyzeSequential(files);
        Report fourPermitReport = new LogAnalyzer(
                4,
                4,
                LockingStrategy.PER_PATH
        ).analyze(files);

        if (!globalReport.equals(perPathReport)) {
            throw new AssertionError("Statistics strategies returned different reports");
        }
        if (!globalReport.equals(sequentialReport)) {
            throw new AssertionError(
                    "Parallel and sequential reports are different"
            );
        }
        if (!globalReport.paths().equals(fourPermitReport.paths())
                || fourPermitReport.maxActiveFiles() > 4
                || fourPermitReport.activeFiles() != 0) {
            throw new AssertionError("Semaphore limit 4 was not respected");
        }
        if (globalReport.totalRequests() != 3
                || globalReport.successfulRequests() != 2
                || globalReport.errorRequests() != 1
                || globalReport.responseBytes() != 60
                || globalReport.totalDurationMillis() != 15
                || globalReport.maxDurationMillis() != 8) {
            throw new AssertionError("Unexpected totals in analysis report");
        }
        if (globalReport.filesProcessed() != 2
                || globalReport.linesRead() != 4
                || globalReport.validRecords() != 3
                || globalReport.malformedRecords() != 1
                || globalReport.readErrors() != 1
                || globalReport.activeFiles() != 0
                || globalReport.maxActiveFiles() > 1
                || globalReport.failedFiles().size() != 1) {
            throw new AssertionError("Unexpected file counters in analysis report");
        }

        LogAnalyzer stoppedAnalyzer = new LogAnalyzer(
                2,
                1,
                LockingStrategy.GLOBAL
        );
        stoppedAnalyzer.stop();
        Report stoppedReport = stoppedAnalyzer.analyze(files);
        if (!stoppedReport.stopped()
                || stoppedReport.totalRequests() != 0
                || stoppedReport.activeFiles() != 0) {
            throw new AssertionError("Stop request was not respected");
        }

        System.out.println(
                "OK: reports match; semaphore limits, stop and bad inputs passed"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;
            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );
            return;
        }

        if (args.length > 0 && args[0].equals("analyze")) {
            if (args.length > 5) {
                throw new IllegalArgumentException(
                        "Usage: java LogAnalysisAssignment analyze "
                                + "<directory> [workers] [fileLimit] [global|perPath]"
                );
            }

            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int workers = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 4;
            int fileLimit = args.length > 3
                    ? Integer.parseInt(args[3])
                    : workers;
            LockingStrategy strategy = LockingStrategy.GLOBAL;
            if (args.length > 4) {
                if (args[4].equalsIgnoreCase("perPath")) {
                    strategy = LockingStrategy.PER_PATH;
                } else if (!args[4].equalsIgnoreCase("global")) {
                    throw new IllegalArgumentException(
                            "Locking strategy must be global or perPath"
                    );
                }
            }

            List<LogFile> files;
            try (var paths = Files.list(directory)) {
                files = paths
                        .filter(Files::isRegularFile)
                        .sorted()
                        .map(LogFile::new)
                        .toList();
            }

            long startTime = System.nanoTime();
            Report report = new LogAnalyzer(
                    workers,
                    fileLimit,
                    strategy
            ).analyze(files);
            long elapsedMillis =
                    (System.nanoTime() - startTime) / 1_000_000;

            System.out.println("Analysis time (ms): " + elapsedMillis);
            System.out.println("Total requests: " + report.totalRequests());
            System.out.println("Successful: " + report.successfulRequests());
            System.out.println("Errors: " + report.errorRequests());
            System.out.println("Response bytes: " + report.responseBytes());
            System.out.println(
                    "Total duration (ms): " + report.totalDurationMillis()
            );
            System.out.println("Maximum duration (ms): " + report.maxDurationMillis());
            System.out.println(
                    "Files processed: " + report.filesProcessed()
                            + ", read errors: " + report.readErrors()
            );
            System.out.println(
                    "Lines: " + report.linesRead()
                            + ", valid: " + report.validRecords()
                            + ", malformed: " + report.malformedRecords()
            );
            System.out.println(
                    "Maximum active files: " + report.maxActiveFiles()
                            + ", active after analysis: " + report.activeFiles()
            );
            System.out.println("Requests by path:");
            for (Map.Entry<String, PathStatistics> entry
                    : report.paths().entrySet()) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue());
            }
            for (String failure : report.failedFiles()) {
                System.err.println("Could not process file: " + failure);
            }
            if (report.stopped()) {
                System.out.println("Analysis was stopped; report is partial.");
            }
            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        System.out.println(
                "Usage: java LogAnalysisAssignment "
                        + "generate <directory> <files> <lines>"
        );
        System.out.println(
                "       java LogAnalysisAssignment check"
        );
        System.out.println(
                "       java LogAnalysisAssignment analyze "
                        + "<directory> [workers] [fileLimit] [global|perPath]"
        );
    }
}
