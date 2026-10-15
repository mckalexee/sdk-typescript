import * as http from 'http';
import * as http2 from 'http2';
import { randomUUID } from 'crypto';
import test from 'ava';
import { Reader } from 'protobufjs/minimal';
import { WorkflowClient } from '@temporalio/client';
import { Runtime } from '@temporalio/worker';
import { TestWorkflowEnvironment, Worker, waitUntil } from './helpers';
import * as workflows from './workflows';

// Field numbers from opentelemetry/proto/collector/metrics/v1/metrics_service.proto and
// opentelemetry/proto/metrics/v1/metrics.proto.
const RESOURCE_METRICS_FIELD = 1; // ExportMetricsServiceRequest.resource_metrics
const SCOPE_METRICS_FIELD = 2; // ResourceMetrics.scope_metrics
const METRICS_FIELD = 2; // ScopeMetrics.metrics
const METRIC_NAME_FIELD = 1; // Metric.name
const METRIC_DATA_FIRST_FIELD = 5; // Metric.data oneof spans fields 5 to 11
const METRIC_DATA_LAST_FIELD = 11;
const HISTOGRAM_FIELD = 9; // Metric.histogram
const EXPONENTIAL_HISTOGRAM_FIELD = 10; // Metric.exponential_histogram

/**
 * Lists the metrics in an OTLP `ExportMetricsServiceRequest` body, with the number of the
 * `Metric.data` field that carries each one.
 */
function exportedMetrics(body: Uint8Array): Array<{ name: string; dataField: number }> {
  const submessages = (buf: Uint8Array, field: number): Uint8Array[] => {
    const found: Uint8Array[] = [];
    const reader = Reader.create(buf);
    while (reader.pos < reader.len) {
      const tag = reader.uint32();
      if (tag >>> 3 === field) found.push(reader.bytes());
      else reader.skipType(tag & 7);
    }
    return found;
  };
  const metrics: Array<{ name: string; dataField: number }> = [];
  for (const resourceMetrics of submessages(body, RESOURCE_METRICS_FIELD)) {
    for (const scopeMetrics of submessages(resourceMetrics, SCOPE_METRICS_FIELD)) {
      for (const metric of submessages(scopeMetrics, METRICS_FIELD)) {
        let name = '';
        let dataField = 0;
        const reader = Reader.create(metric);
        while (reader.pos < reader.len) {
          const tag = reader.uint32();
          const field = tag >>> 3;
          if (field === METRIC_NAME_FIELD) {
            name = reader.string();
          } else {
            if (field >= METRIC_DATA_FIRST_FIELD && field <= METRIC_DATA_LAST_FIELD) dataField = field;
            reader.skipType(tag & 7);
          }
        }
        metrics.push({ name, dataField });
      }
    }
  }
  return metrics;
}

async function withFakeGrpcServer(
  fn: (port: number) => Promise<void>,
  requestListener?: (request: http2.Http2ServerRequest, response: http2.Http2ServerResponse) => void
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const srv = http2.createServer();
    srv.listen({ port: 0, host: '127.0.0.1' }, () => {
      const addr = srv.address();
      if (typeof addr === 'string' || addr === null) {
        throw new Error('Unexpected server address type');
      }
      srv.on('request', async (req, res) => {
        if (requestListener) requestListener(req, res);
        res.statusCode = 200;
        res.addTrailers({
          'grpc-status': '0',
          'grpc-message': 'OK',
        });
        res.write(
          // This is a raw gRPC response, of length 0
          new Uint8Array([
            // Frame Type: Data; Not Compressed
            0,
            // Message Length: 0
            0, 0, 0, 0,
          ])
        );
        res.end();
      });
      fn(addr.port)
        .catch((e) => reject(e))
        .finally(() => {
          resolve();

          // The OTel exporter will try to flush metrics on drop, which may result in tons of ERROR
          // messages on the console if the server has had time to complete shutdown before then.
          // Delaying closing the server by 1 second is enough to avoid that situation, and doesn't
          // need to be awaited, so that doesn't slow down tests.
          setTimeout(() => {
            srv.close();
          }, 1000).unref();
        });
    });
  });
}

async function withHttpServer(
  fn: (port: number) => Promise<void>,
  requestListener?: (request: http.IncomingMessage) => void
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const srv = http.createServer();
    srv.listen({ port: 0, host: '127.0.0.1' }, () => {
      const addr = srv.address();
      if (typeof addr === 'string' || addr === null) {
        throw new Error('Unexpected server address type');
      }
      srv.on('request', async (req, res) => {
        if (requestListener) await requestListener(req);
        res.statusCode = 200;
        res.end();
      });
      fn(addr.port)
        .catch((e) => reject(e))
        .finally(() => {
          resolve();

          // The OTel exporter will try to flush metrics on drop, which may result in tons of ERROR
          // messages on the console if the server has had time to complete shutdown before then.
          // Delaying closing the server by 1 second is enough to avoid that situation, and doesn't
          // need to be awaited, so that doesn't slow down tests.
          setTimeout(() => {
            srv.close();
          }, 1000).unref();
        });
    });
  });
}

test.serial('Runtime.install() throws meaningful error when passed invalid metrics.otel.url', async (t) => {
  t.throws(() => Runtime.install({ telemetryOptions: { metrics: { otel: { url: ':invalid' } } } }), {
    instanceOf: TypeError,
    message: /metricsExporter.otel.url/,
  });
});

test.serial(
  'Runtime.install() throws meaningful error when metrics.otel.histogramAggregation is combined with histogramBucketOverrides',
  async (t) => {
    t.throws(
      () =>
        Runtime.install({
          telemetryOptions: {
            metrics: {
              otel: {
                url: 'http://127.0.0.1:1234',
                histogramBucketOverrides: { request_latency: [1, 5, 10] },
                histogramAggregation: { type: 'exponential' },
              },
            },
          },
        }),
      { instanceOf: Error, message: /histogram_bucket_overrides/ }
    );
  }
);

test.serial('Runtime.install() throws meaningful error when passed invalid metrics.otel.histogramAggregation', async (t) => {
  t.throws(
    () =>
      Runtime.install({
        telemetryOptions: {
          metrics: {
            otel: {
              url: 'http://127.0.0.1:1234',
              histogramAggregation: { type: 'bogus' } as any,
            },
          },
        },
      }),
    { instanceOf: TypeError, message: /metrics.otel.histogramAggregation/ }
  );
});

test.serial('Runtime.install() accepts metrics.otel.url without headers', async (t) => {
  try {
    Runtime.install({ telemetryOptions: { metrics: { otel: { url: 'http://127.0.0.1:1234' } } } });
    t.pass();
  } finally {
    // Cleanup the runtime so that it doesn't interfere with other tests
    await Runtime._instance?.shutdown();
  }
});

test.serial('Exporting OTEL metrics from Core works', async (t) => {
  let resolveCapturedRequest = (_req: http2.Http2ServerRequest) => undefined as void;
  const capturedRequest = new Promise<http2.Http2ServerRequest>((r) => (resolveCapturedRequest = r));
  try {
    await withFakeGrpcServer(async (port: number) => {
      Runtime.install({
        telemetryOptions: {
          metrics: {
            otel: {
              url: `http://127.0.0.1:${port}`,
              headers: {
                'x-test-header': 'test-value',
              },
              metricsExportInterval: 10,
            },
          },
        },
      });

      const localEnv = await TestWorkflowEnvironment.createLocal();
      try {
        const worker = await Worker.create({
          connection: localEnv.nativeConnection,
          workflowsPath: require.resolve('./workflows'),
          taskQueue: 'test-otel',
        });
        const client = new WorkflowClient({
          connection: localEnv.connection,
        });
        await worker.runUntil(async () => {
          await client.execute(workflows.successString, {
            taskQueue: 'test-otel',
            workflowId: randomUUID(),
          });
          const req = await Promise.race([
            capturedRequest,
            new Promise<undefined>((resolve) => setTimeout(() => resolve(undefined), 5000)),
          ]);
          t.truthy(req);
          t.is(req?.url, '/opentelemetry.proto.collector.metrics.v1.MetricsService/Export');
          t.is(req?.headers['x-test-header'], 'test-value');
        });
      } finally {
        await localEnv.teardown();
      }
    }, resolveCapturedRequest);
  } finally {
    // Cleanup the runtime so that it doesn't interfere with other tests
    await Runtime._instance?.shutdown();
  }
});

test.serial('Exporting OTEL metrics using OTLP/HTTP from Core works', async (t) => {
  let resolveCapturedRequest = (_req: http.IncomingMessage) => undefined as void;
  const capturedRequest = new Promise<http.IncomingMessage>((r) => (resolveCapturedRequest = r));
  try {
    await withHttpServer(async (port: number) => {
      Runtime.install({
        telemetryOptions: {
          metrics: {
            otel: {
              url: `http://127.0.0.1:${port}/v1/metrics`,
              http: true,
              headers: {
                'x-test-header': 'test-value',
              },
              metricsExportInterval: 10,
            },
          },
        },
      });

      const localEnv = await TestWorkflowEnvironment.createLocal();
      try {
        const worker = await Worker.create({
          connection: localEnv.nativeConnection,
          workflowsPath: require.resolve('./workflows'),
          taskQueue: 'test-otel',
        });
        const client = new WorkflowClient({
          connection: localEnv.connection,
        });
        await worker.runUntil(async () => {
          await client.execute(workflows.successString, {
            taskQueue: 'test-otel',
            workflowId: randomUUID(),
          });
          const req = await Promise.race([
            capturedRequest,
            new Promise<undefined>((resolve) => setTimeout(() => resolve(undefined), 5000)),
          ]);
          t.truthy(req);
          t.is(req?.url, '/v1/metrics');
          t.is(req?.headers['x-test-header'], 'test-value');
        });
      } finally {
        await localEnv.teardown();
      }
    }, resolveCapturedRequest);
  } finally {
    // Cleanup the runtime so that it doesn't interfere with other tests
    await Runtime._instance?.shutdown();
  }
});

test.serial('Exporting OTEL metrics as exponential histograms from Core works', async (t) => {
  const exported: Array<{ name: string; dataField: number }> = [];
  try {
    await withHttpServer(
      async (port: number) => {
        Runtime.install({
          telemetryOptions: {
            metrics: {
              otel: {
                url: `http://127.0.0.1:${port}/v1/metrics`,
                http: true,
                metricsExportInterval: 10,
                histogramAggregation: { type: 'exponential', nameSuffix: '_exp' },
              },
            },
          },
        });

        const localEnv = await TestWorkflowEnvironment.createLocal();
        try {
          const worker = await Worker.create({
            connection: localEnv.nativeConnection,
            workflowsPath: require.resolve('./workflows'),
            taskQueue: 'test-otel-exponential',
          });
          const client = new WorkflowClient({
            connection: localEnv.connection,
          });
          await worker.runUntil(async () => {
            await client.execute(workflows.successString, {
              taskQueue: 'test-otel-exponential',
              workflowId: randomUUID(),
            });
            // The first exports may carry only counters and gauges, so wait for a histogram of either kind.
            await waitUntil(
              async () =>
                exported.some((m) => m.dataField === HISTOGRAM_FIELD || m.dataField === EXPONENTIAL_HISTOGRAM_FIELD),
              5000
            );
          });
        } finally {
          await localEnv.teardown();
        }
      },
      async (req) => {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(chunk as Buffer);
        exported.push(...exportedMetrics(Buffer.concat(chunks)));
      }
    );
  } finally {
    // Cleanup the runtime so that it doesn't interfere with other tests
    await Runtime._instance?.shutdown();
  }

  const histograms = exported.filter(
    (m) => m.dataField === HISTOGRAM_FIELD || m.dataField === EXPONENTIAL_HISTOGRAM_FIELD
  );
  t.true(histograms.length > 0, 'no histograms were exported');
  for (const { name, dataField } of histograms) {
    t.is(dataField, EXPONENTIAL_HISTOGRAM_FIELD, `${name} was exported as a fixed-bucket histogram`);
    t.true(name.endsWith('_exp'), `${name} is missing the name suffix`);
  }
});
