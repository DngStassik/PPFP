import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Mandelbrot {

    private static final int DEFAULT_WIDTH = 1200;
    private static final int DEFAULT_HEIGHT = 800;
    private static final int DEFAULT_MAX_ITERATIONS = 1000;
    private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();

    private static final double MIN_REAL = -2.0;
    private static final double MAX_REAL = 1.0;
    private static final double MIN_IMAGINARY = -1.2;
    private static final double MAX_IMAGINARY = 1.2;

    private Mandelbrot() {
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        if (args.length > 5) {
            throw new IllegalArgumentException(
                    "Usage: java Mandelbrot [width] [height] [maxIterations] [threads] [output.ppm]");
        }

        int width = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_WIDTH;
        int height = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_HEIGHT;
        int maxIterations = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_MAX_ITERATIONS;
        int threadCount = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_THREADS;

        if (width < 2 || height < 2 || maxIterations < 1 || threadCount < 1) {
            throw new IllegalArgumentException(
                    "width and height must be at least 2; maxIterations and threads must be positive");
        }

        long start = System.nanoTime();
        boolean[][] inside = calculate(width, height, maxIterations, threadCount);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        long insideCount = 0;
        for (boolean[] row : inside) {
            for (boolean pixelInside : row) {
                if (pixelInside) {
                    insideCount++;
                }
            }
        }

        System.out.printf("Image: %d x %d, threads: %d%n", width, height, threadCount);
        System.out.printf("Pixels in the set: %d; calculation time: %d ms%n", insideCount, elapsedMillis);

        if (args.length > 4) {
            writeImage(inside, Path.of(args[4]));
            System.out.println("Image saved to: " + args[4]);
        }
    }

    private static boolean[][] calculate(int width, int height, int maxIterations, int threadCount)
            throws InterruptedException, ExecutionException {
        boolean[][] inside = new boolean[height][width];
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> rows = new ArrayList<>(height);

        try {
            for (int y = 0; y < height; y++) {
                final int row = y;
                rows.add(executor.submit(() -> {
                    for (int x = 0; x < width; x++) {
                        double real = MIN_REAL + x * (MAX_REAL - MIN_REAL) / (width - 1);
                        double imaginary = MAX_IMAGINARY
                                - row * (MAX_IMAGINARY - MIN_IMAGINARY) / (height - 1);
                        inside[row][x] = isInSet(real, imaginary, maxIterations);
                    }
                }));
            }

            for (Future<?> row : rows) {
                row.get();
            }
        } finally {
            executor.shutdown();
        }

        return inside;
    }

    private static boolean isInSet(double real, double imaginary, int maxIterations) {
        double zReal = 0;
        double zImaginary = 0;

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            double zRealSquared = zReal * zReal;
            double zImaginarySquared = zImaginary * zImaginary;
            if (zRealSquared + zImaginarySquared > 4) {
                return false;
            }

            double nextImaginary = 2 * zReal * zImaginary + imaginary;
            zReal = zRealSquared - zImaginarySquared + real;
            zImaginary = nextImaginary;
        }

        return zReal * zReal + zImaginary * zImaginary <= 4;
    }

    private static void writeImage(boolean[][] inside, Path output) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.US_ASCII)) {
            writer.write("P3\n");
            writer.write(inside[0].length + " " + inside.length + "\n255\n");

            for (boolean[] row : inside) {
                for (boolean pixelInside : row) {
                    int shade = pixelInside ? 0 : 255;
                    writer.write(shade + " " + shade + " " + shade + " ");
                }
                writer.newLine();
            }
        }
    }
}
