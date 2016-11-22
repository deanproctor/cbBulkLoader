import com.couchbase.client.java.document.JsonDocument;

/**
 * Created by David Ostrovsky on 1/25/2016.
 */
public class CouchbaseOperationResult {
    private String id;
    private JsonDocument result;
    private Throwable error;
    private CouchbaseOperationResultStatus status;

    public CouchbaseOperationResult(String id, JsonDocument result, Throwable error, CouchbaseOperationResultStatus status) {
        this.id = id;
        this.result = result;
        this.error = error;
        this.status = status;
    }

    public JsonDocument getResult() {
        return result;
    }

    public void setResult(JsonDocument result) {
        this.result = result;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public CouchbaseOperationResultStatus getStatus() {
        return status;
    }

    public void setStatus(CouchbaseOperationResultStatus status) {
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
