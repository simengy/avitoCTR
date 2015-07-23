import pandas as pd

preds = pd.read_csv('temp/temp.csv', header = None)
print preds.shape
sample = pd.read_csv('../input/sampleSubmission.csv', nrows=preds.shape[0])

sample['IsClick'] = preds.iloc[:,0]
sample.to_csv('results/submission_1.csv', index=False)
