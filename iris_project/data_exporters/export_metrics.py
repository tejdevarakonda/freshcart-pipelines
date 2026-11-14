# iris_project/data_exporters/export_metrics.py
import os
import pandas as pd

if 'data_exporter' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_exporter
    except ImportError:
        def data_exporter(func):
            return func

if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(func):
            return func


@data_exporter
def export_metrics(metrics_df, *args, **kwargs):
    """
    Custom block for Part C4: Exports evaluation metrics to CSV.
    Receives metrics DataFrame from evaluate_model and persists to iris_metrics.csv.
    """
    output_path = 'iris_metrics.csv'
    metrics_df.to_csv(output_path, index=False)
    print(f'Saved {len(metrics_df)} metrics to {output_path}')
    return metrics_df


@test
def test_metrics_integrity(output, *args):
    """
    Quality gate for export_metrics:
    Verifies that the metrics dataframe contains all 4 standard metrics
    and that all scores fall within the valid probability range [0.0, 1.0].
    """
    assert len(output) == 4, f'Expected 4 evaluation metrics, got {len(output)}'
    expected_metrics = {'accuracy', 'precision', 'recall', 'f1_score'}
    actual_metrics = set(output['metric'].tolist())
    assert expected_metrics.issubset(actual_metrics), f'Missing metrics: {expected_metrics - actual_metrics}'
    assert (output['value'] >= 0.0).all() and (output['value'] <= 1.0).all(), 'Metric values must be between 0 and 1'
