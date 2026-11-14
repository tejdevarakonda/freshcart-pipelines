"""
Part C: Mage ML Pipeline Execution & Verification Engine
=========================================================
Orchestrates:
- load_iris (data_loader) + test_output
- prepare_data (transformer) + test_output
- train_model (transformer) + test_accuracy gate
- save_results (data_exporter)
- evaluate_model (transformer) -> evaluation branch
- export_metrics (data_exporter) -> C4 custom block + test_metrics_integrity
"""

import os
import sys
import pandas as pd
import numpy as np

# Add repository root to Python path
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import iris_project.data_loaders.load_iris as loader_module
import iris_project.transformers.prepare_data as prep_module
import iris_project.transformers.train_model as train_module
import iris_project.data_exporters.save_results as save_module
import iris_project.transformers.evaluate_model as eval_module
import iris_project.data_exporters.export_metrics as custom_module


def run_pipeline(max_depth=3, include_eval_branch=False, include_custom_block=False):
    """
    Executes the Mage pipeline DAG adhering to metadata.yaml orchestration rules:
    Each block executes its transform/load function, followed immediately by its @test.
    If a test fails, an AssertionError is raised, gating downstream blocks.
    """
    print(f"\n{'='*60}")
    print(f"RUNNING MAGE PIPELINE: iris_classifier (runtime-vars: max_depth={max_depth})")
    print(f"{'='*60}")

    # 1. Block: load_iris
    print("\n[Block 1/4] Executing: load_iris (data_loader)...")
    df_raw = loader_module.load_data()
    print("Running @test: test_output...")
    loader_module.test_output(df_raw)
    print("PASS: load_iris test passed (150 rows verified).")

    # 2. Block: prepare_data
    print("\n[Block 2/4] Executing: prepare_data (transformer)...")
    df_prepared = prep_module.transform(df_raw)
    print("Running @test: test_output...")
    prep_module.test_output(df_prepared)
    print("PASS: prepare_data test passed ('split' column present).")

    # 3. Block: train_model (Test Gate)
    print(f"\n[Block 3/4] Executing: train_model (transformer, max_depth={max_depth})...")
    df_predictions = train_module.train(df_prepared, max_depth=max_depth)
    
    # Calculate accuracy
    acc = (df_predictions['predicted'] == df_predictions['target']).mean()
    test_passed = False
    
    print("Running @test: test_accuracy (Threshold: accuracy >= 0.8)...")
    try:
        train_module.test_accuracy(df_predictions)
        print(f"PASS: train_model test passed (Accuracy: {acc:.3f} >= 0.8)")
        test_passed = True
    except AssertionError as e:
        print(f"FAIL: train_model test FAILED! ({e})")
        print("PIPELINE EXECUTION BLOCKED: Downstream blocks (save_results) will NOT run.")
        return {
            'max_depth': max_depth,
            'accuracy': acc,
            'test_passed': False,
            'csv_created': False
        }

    # 4. Block: save_results (Only reached if test passes!)
    print("\n[Block 4/4] Executing: save_results (data_exporter)...")
    save_module.export_data(df_predictions)
    csv_exists = os.path.exists('iris_predictions.csv')
    print(f"iris_predictions.csv exists: {csv_exists}")

    metrics_df = None
    if include_eval_branch:
        print("\n[Branch] Executing: evaluate_model (transformer)...")
        metrics_df = eval_module.evaluate(df_predictions)

    if include_custom_block and metrics_df is not None:
        print("\n[Custom Block C4] Executing: export_metrics (data_exporter)...")
        custom_module.export_metrics(metrics_df)
        print("Running @test: test_metrics_integrity...")
        custom_module.test_metrics_integrity(metrics_df)
        print("PASS: export_metrics test passed.")

    return {
        'max_depth': max_depth,
        'accuracy': acc,
        'test_passed': test_passed,
        'csv_created': csv_exists
    }


def main():
    print("="*60)
    print("EXPERIMENT C1: Baseline Pipeline Run (max_depth=3)")
    print("="*60)
    if os.path.exists('iris_predictions.csv'):
        os.remove('iris_predictions.csv')
    res_c1 = run_pipeline(max_depth=3)
    
    print("\n" + "="*60)
    print("EXPERIMENT C2: Parameter Sweep & Test-Gate Verification (max_depth=1..5)")
    print("="*60)
    
    sweep_results = []
    for depth in [1, 2, 3, 4, 5]:
        if os.path.exists('iris_predictions.csv'):
            os.remove('iris_predictions.csv')
        
        res = run_pipeline(max_depth=depth)
        sweep_results.append(res)

    print("\n" + "="*60)
    print("C2 PARAMETER SWEEP RESULTS TABLE:")
    print("="*60)
    sweep_df = pd.DataFrame(sweep_results)
    sweep_df.columns = ['max_depth', 'Test accuracy', 'test_accuracy passed?', 'iris_predictions.csv created?']
    print(sweep_df.to_string(index=False))

    print("\n" + "="*60)
    print("EXPERIMENT C3 & C4: Evaluation Branch & Custom Block")
    print("="*60)
    if os.path.exists('iris_predictions.csv'):
        os.remove('iris_predictions.csv')
    if os.path.exists('iris_metrics.csv'):
        os.remove('iris_metrics.csv')
    
    res_c3_c4 = run_pipeline(max_depth=3, include_eval_branch=True, include_custom_block=True)
    print(f"\nFiles created: iris_predictions.csv ({os.path.exists('iris_predictions.csv')}), iris_metrics.csv ({os.path.exists('iris_metrics.csv')})")


if __name__ == '__main__':
    main()
