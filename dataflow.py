import argparse
from datetime import datetime
import logging
import random
import json
from uuid import uuid4

from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """

    def __init__(self, window_size, num_shards=5):
        self.window_size = window_size
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size), allowed_lateness=30*24*60*60)  # 30 days lateness
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )


class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )


class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=DoFn.WindowParam):
        """Write messages in a batch to Google Cloud Storage."""

        uuid = str(uuid4())
        window_start = window.start.to_utc_datetime().strftime("%Y%m%d_%H-%M-%S")
        window_end = window.end.to_utc_datetime().strftime("%M-%S")
        shard_id, batch = key_value
        filename = "-".join([self.output_path, window_start, window_end, str(shard_id), uuid])

        data = []

        for message_body, publish_time in batch:
            row = json.loads(message_body)
            row['_event_metadata'] = dict(publish_time=publish_time)
            row = json.dumps(row)
            data.append(row)

        data = "\n".join(data).encode("utf-8")

        with io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            f.write(data)


def run(input_subscription, output_path, job_name, window_size=1.0, num_shards=5, pipeline_args=None):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True, job_name=job_name
    )

    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
            # binds the publish time returned by the Pub/Sub server for each message
            # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
            | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription=input_subscription , id_label='message_id')
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards)
            | "Write to GCS" >> ParDo(WriteToGCS(output_path))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_subscription",
        help="Subscription path",
    )

    parser.add_argument(
        "--job_name",
        help="Job name",
    )

    parser.add_argument(
        "--window_size",
        type=int,
        default=15,
        help="Output file's window size in seconds.",
    )
    parser.add_argument(
        "--output_path",
        help="Path of the output GCS file including the prefix.",
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=3,
        help="Number of shards to use when writing windowed elements to GCS.",
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_subscription,
        known_args.output_path,
        known_args.job_name,
        known_args.window_size,
        known_args.num_shards,
        pipeline_args,
    )

# --project=jokr-global   
# --region=europe-west4   
# --input_topic=projects/jokr-global/topics/bringg-tasks   
# --output_path=gs://jokr-bringg/tasks/   
# --runner=DataflowRunner   
# --window_size=20   
# --num_shards=1 
# --temp_location=gs://jokr-bringg/temp/  
# --input_subscription=projects/jokr-global/subscriptions/bringg-tasks-datalake-feed 
# --job_name=bringg-tasks-datalake-job
# --service_account_email=streaming-service@jokr-global.iam.gserviceaccount.com