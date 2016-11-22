import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.time.Delay;
import rx.Observable;
import rx.schedulers.Schedulers;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;


public class cbBulkLoader {
    public static <D extends Document<?>> void main(String[] args) {

    	CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
			//.kvEndpoints(64)
			//.ioPoolSize(64)
			//.computationPoolSize(64)
			//.requestBufferSize(131072)
			//.responseBufferSize(131072)
			//.kvTimeout(250)
			.build();
        Cluster cluster = CouchbaseCluster.create(env, "192.168.61.101");

        Bucket bucket = cluster.openBucket("bulk-test");

        int docsToUpdate = 1000000;
        List <String> documents = new ArrayList <String> ();
        for (int i = 0; i < docsToUpdate; i++) {
            documents.add("doc-" + i);
        }

        List<CouchbaseOperationResult> results =
            Observable
            .from(documents)
            .map(key -> new CouchbaseOperationResult(key, null, null, CouchbaseOperationResultStatus.NOT_FOUND))
            .flatMap(op -> bucket.async().get(op.getId())
                // Update the result with a success and the retrieved document
                .map(doc -> {
                    op.setResult(doc);
                    op.setStatus(CouchbaseOperationResultStatus.SUCCESS);
                    return op;
                })
                // Try to get from replica if unable to get result
                .onErrorResumeNext(throwable -> {
                    // Can't retrieve anything, return a failure status
                    op.setStatus(CouchbaseOperationResultStatus.FAILURE);
                    return Observable.just(op);
                })
                // If the result is empty (document not found) return the result object itself
                .defaultIfEmpty(op))
            // Business logic goes here
            .filter(cbBulkLoader::Filter)
            // Mutate the document
            .map(cbBulkLoader::UpdateDocument)
            .flatMap(op -> {
                // If we're carrying a failure status from an earlier stage, just pass it through
                if(op.getStatus() == CouchbaseOperationResultStatus.FAILURE)
                    return Observable.just(op);

                // If document does not exist, insert it
                if(op.getStatus() == CouchbaseOperationResultStatus.NOT_FOUND)
                    return bucket.async().insert(GenerateNewDocument(op.getId()))
                         .map(doc -> {
                             op.setResult(doc);
                             op.setStatus(CouchbaseOperationResultStatus.SUCCESS);
                             return (CouchbaseOperationResult)op;
                         }).onErrorReturn(throwable -> {
                            op.setError(throwable);
                            op.setStatus(CouchbaseOperationResultStatus.FAILURE);
                            return op;
                        });

                // Try to update
                return bucket.async().replace(op.getResult())
                    .map(doc ->  {
                        op.setResult(doc);
                        op.setStatus(CouchbaseOperationResultStatus.SUCCESS);
                        return op;
                    })
                    .onErrorReturn(throwable -> {
                        op.setError(throwable);
                        op.setStatus(CouchbaseOperationResultStatus.FAILURE);
                        return op;
                    });
                }
            )
            .toList()
            .toBlocking()
            .single();

        results.stream()
                .filter(result -> result.getStatus() != CouchbaseOperationResultStatus.SUCCESS)
                .forEach(result -> System.out.println(result.getId() + " failed with error: " + result.getError()));

        List<String> failedIds = results.stream()
                .filter(op -> op.getStatus() != CouchbaseOperationResultStatus.SUCCESS)
                .map(CouchbaseOperationResult::getId)
                .collect(Collectors.toList());

        cluster.disconnect();
    }

    private static JsonDocument GenerateNewDocument(String id) {
        // Override new document creation here:
        return JsonDocument.create(id, JsonObject.create().put("counter", 0));
    }

    private static CouchbaseOperationResult UpdateDocument(CouchbaseOperationResult op) {
        if(op.getStatus() != CouchbaseOperationResultStatus.SUCCESS ||
           op.getResult() == null)
            return op;

        if(op.getResult().content() == null)
            op.setResult(JsonDocument.create(op.getId()));

        if(op.getResult().content().get("counter") == null) {
            op.getResult().content().put("counter", 0);
        } else {
            op.getResult().content().put("counter", op.getResult().content().getInt("counter") + 1);
        }
        return op;
    }

    private static Boolean Filter(CouchbaseOperationResult op) {
        if( op.getStatus() != CouchbaseOperationResultStatus.SUCCESS ||
            op.getResult() == null ||
            op.getResult().content() == null)
            return true;

        return op.getResult().content().get("counter") == null || op.getResult().content().getInt("counter") < 100;
    }
}
