package kr.co.idgenerator;

public class BenchmarkResult {
    private final String generatorName;
    private final int sampleSize;
    private int byteSize;
    private long generationTime;
    private double collisionRate;
    private long dbInsertTime;
    private String exampleId;

    BenchmarkResult(final String generatorName, final int sampleSize) {
        this.generatorName = generatorName;
        this.sampleSize = sampleSize;
    }

    public String getGeneratorName() {
        return generatorName;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public int getByteSize() {
        return byteSize;
    }

    public void setByteSize(final int byteSize) {
        this.byteSize = byteSize;
    }


    public long getGenerationTime() {
        return generationTime;
    }

    public void setGenerationTime(final long generationTime) {
        this.generationTime = generationTime;
    }

    public double getCollisionRate() {
        return collisionRate;
    }

    public void setCollisionRate(final double collisionRate) {
        this.collisionRate = collisionRate;
    }

    public long getDbInsertTime() {
        return dbInsertTime;
    }

    public void setDbInsertTime(final long dbInsertTime) {
        this.dbInsertTime = dbInsertTime;
    }

    public String getExampleId() {
        return exampleId;
    }

    public void setExampleId(final String exampleId) {
        this.exampleId = exampleId;
    }
}
