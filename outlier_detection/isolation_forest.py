import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
from sklearn.model_selection import ParameterGrid
import matplotlib.pyplot as plt

def generate_synthetic_data():
    np.random.seed(42)
    data = np.concatenate([
        np.random.normal(0, 1, size=(100, 2)),  # Normal data
        np.random.normal(5, 1, size=(10, 2))   # Outliers
    ])
    columns = ["Feature1", "Feature2"]
    df = pd.DataFrame(data, columns=columns)
    df['true_labels'] = [1] * 100 + [-1] * 10  # 1 = inlier, -1 = outlier
    return df

def tune_isolation_forest(data, columns):
    param_grid = {
        'n_estimators': [50, 100, 200],
        'contamination': [0.05, 0.1, 0.2],
        'max_samples': [0.5, 1.0],
        'max_features': [0.5, 1.0]
    }

    best_params = None
    best_auc = -1

    for params in ParameterGrid(param_grid):
        iso_forest = IsolationForest(
            n_estimators=params['n_estimators'],
            contamination=params['contamination'],
            max_samples=params['max_samples'],
            max_features=params['max_features'],
            random_state=42
        )

        data['anomaly'] = iso_forest.fit_predict(data[columns])
        y_pred = data['anomaly']
        auc = roc_auc_score(data['true_labels'].map({1: 1, -1: 0}), y_pred.map({1: 1, -1: 0}))
        print(f"Params: {params}, AUC: {auc}")

        if auc > best_auc:
            best_auc = auc
            best_params = params

    print(f"\nBest Parameters: {best_params}")
    print(f"Best AUC Score: {best_auc}")
    return best_params

def train_final_model(data, columns, best_params):
    iso_forest = IsolationForest(
        n_estimators=best_params['n_estimators'],
        contamination=best_params['contamination'],
        max_samples=best_params['max_samples'],
        max_features=best_params['max_features'],
        random_state=42
    )
    data['anomaly'] = iso_forest.fit_predict(data[columns])
    return data

def evaluate_model(data):
    y_true = data['true_labels']
    y_pred = data['anomaly']

    print("\nConfusion Matrix:")
    print(confusion_matrix(y_true, y_pred))

    print("\nClassification Report:")
    print(classification_report(y_true, y_pred, target_names=["Inlier", "Outlier"]))

    fpr, tpr, _ = roc_curve(y_true.map({1: 1, -1: 0}), y_pred.map({1: 1, -1: 0}))
    auc = roc_auc_score(y_true.map({1: 1, -1: 0}), y_pred.map({1: 1, -1: 0}))
    print(f"\nAUC Score: {auc}")

    plt.figure(figsize=(8, 6))
    plt.plot(fpr, tpr, label=f"ROC Curve (AUC = {auc:.2f})")
    plt.plot([0, 1], [0, 1], linestyle="--", color="gray")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curve")
    plt.legend()
    plt.grid()
    plt.show()

def save_filtered_data(data):
    inliers = data[data['anomaly'] == 1].copy()
    inliers.drop(columns=['anomaly'], inplace=True)
    inliers.to_csv("inliers.csv", index=False)

def main():
    data = generate_synthetic_data()
    columns = ["Feature1", "Feature2"]
    best_params = tune_isolation_forest(data, columns)
    data = train_final_model(data, columns, best_params)
    evaluate_model(data)
    save_filtered_data(data)

if __name__ == "__main__":
    main()
