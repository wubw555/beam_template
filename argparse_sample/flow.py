import json
import argparse
import apache_beam as beam


def main():
    parser = argparse.ArgumentParser(description='Awesome Pipeline')
    parser.add_argument('--project', required=True,
                        help='Your Google Cloud Project')
    parser.add_argument('--topic', required=True, help='Your PubSub topic')
    parser.add_argument('--bucket', required=True,
                        help='Your tmp/test Input GCS')
    parser.add_argument('--dataset', required=True,
                        help='Your Output BigQuery Dataset')
    parser.add_argument('--table', required=True,
                        help='Your Output BigQuery Table')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--DirectRunner', action='store_true',
                       help='Your Pipeline Runner')
    group.add_argument('--DataflowRunner', action='store_true',
                       help='Your Pipeline Runner')
    args = parser.parse_args()

    if args.DirectRunner:
        runner = 'DirectRunner'
    else:
        runner = 'DataflowRunner'

    settings = [
        f'--project={args.project}',
        '--job_name=job1',
        f'--staging_location=gs://{args.bucket}/staging/',
        f'--temp_location=gs://{args.bucket}/tmp/',
        f'--runner={runner}',
        '--region=asia-northeast1',
        '--max_num_workers=1'
    ]

    if not args.DirectRunner:
        settings.append('--streaming')

    p = beam.Pipeline(argv=settings)

    if args.DirectRunner:
        pcoll = p | 'ReadFromGCS' >> beam.io.ReadFromText(
            f'gs://{args.bucket}/test/test.json')
    else:
        pcoll = p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
            topic=f'projects/{args.project}/topics/{args.topic}')

    (pcoll
        | 'LoadJSON' >> beam.Map(json.loads)
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            project=args.project,
            dataset=args.dataset,
            table=args.table,
            schema='id:INTEGER,name:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
     )

    if args.DirectRunner:
        p.run().wait_until_finish()
    else:
        p.run()


if __name__ == '__main__':
    main()
