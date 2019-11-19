# from snorkel.labeling import LabelModel, PandasLFApplier, labeling_function
# from sklearn.model_selection import train_test_split
# from snorkel.preprocess import preprocessor
# from snorkel.labeling import LFAnalysis
import subprocess
import numpy as np
import requests
import re
import pdb
import os
import pandas as pd
import sys
import argparse
# import os
import csv
# import time
# from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
# from sklearn.linear_model import LogisticRegression
# from keras.models import Sequential
# from keras import layers
# import matplotlib.pyplot as plt
# from keras.preprocessing.text import Tokenizer
import time
#
# COMPANY = 1
# NOTCOMPANY = 0
# ABSTAIN = -1


# source ~/.virtualenvs/NLP-Classifier-6qAEZNCR/bin/activate

def connect_to_orgclassifier():
    """
    Connects to the orgtype_classifier API (localhost server) on port 8080
    """
    print("Killing port 8080 connection")
    subprocess.run(['kill $(lsof -t -i:8080)'], shell=True)
    p =subprocess.Popen(['python server.py model.pkl.gz'], cwd='.', shell=True)

    time.sleep(3)
    return p


# @preprocessor(memoize=True)
def classify_org(x):
    try:
        org_string = x['reg_name']
        # Strip whitespace and replace spaces with %20 (for the url)
        org_string = org_string.strip()
        org_string = re.sub(r'\s+', '%20', org_string)

        url = r'http://localhost:8080/predict?q=' + str(org_string)
        resp = requests.get(url)

        # Convert requests response object to python dict
        x['org_label'] = resp.text
    except ValueError:

        print(ValueError)
        x['org_label'] = ''

    return x['org_label']


def combine_training_files(args):

    combinedir = args.combinedir
    traindir = args.trainingdir

    with open(os.path.join(combinedir, 'combinedtrain.csv'), 'w+') as combo:
        # writer = csv.writer(combo, delimiter=',')
        for f in os.listdir(traindir):
            if f.endswith('.csv'):
                cls = os.path.splitext(f)[0]
                df = pd.read_csv(os.path.join(traindir, f), usecols=['org_string'])
                df['cls'] = cls
                df.drop_duplicates(['org_string'], inplace=True)
                df.to_csv(combo, mode='a', index=False)


def filtercombinedfileforunseenstrings(df, args):

    combined = pd.read_csv(os.path.join(args.combinedir, 'combinedtrain.csv'))
    df_filt = pd.merge(df, combined, how='left', right_on='org_string', left_on='reg_name')
    # Filter for unmatched (strings not used to train classifier)
    df_filt = df_filt[pd.isnull(df_filt['org_string'])]
    df_filt.to_csv(os.path.join(args.datadir, 'filtered_unseen.csv'), index=False)

# @labeling_function(pre=[classify_org])
# @labeling_function()
# def lf_iscompany(x):
#
#     comp_or_not_dict = {'Private Limited Company': 'Company',
#                         'Company Limited by Guarantee': 'Company',
#                         'Royal Charter Company': 'Company',
#                         'Community Interest Company': 'Not A Company',
#                         'Registered Society': 'Not A Company',
#                         'Registered charity': 'Not A Company',
#                         'Individual': 'Not A Company',
#                         'Government': 'Not A Company',
#                         'School': 'Not A Company',
#                         'Community Amateur Sports Club': 'Not A Company',
#                         'Local Authority': 'Not A Company',
#                         'Parish or Town Council': 'Not A Company',
#                         'University': 'Not A Company'}
#
#     x['company_or_not'] = comp_or_not_dict[x['org_label']]
#
#     if x.company_or_not == 'Company':
#         return COMPANY
#
#     elif x.company_or_not == 'Not A Company':
#         return NOTCOMPANY
#     else:
#         return ABSTAIN


# @labeling_function()
# def lf_regex_check_out(x):
#
#     """Spam comments say 'check out my video', 'check it out', etc."""
#     return COMPANY if re.search(r"ltd", x.src_name, flags=re.I) else NOTCOMPANY

#
# def loadsplitdata(dfx):
#
#     # Split dfx into 80:20 test:train sets
#     df_train, df_test = train_test_split(dfx, test_size=0.2)
#
#     # Split resulting df_train (80% above) again into 80:20 train:validation sets (sklearn has no ability to split in one go)
#     df_train, df_valid = train_test_split(df_train, test_size = 0.2)
#
#     return df_train, df_valid, df_test


def main():
    # source /Users/davidmellor/.local/share/virtualenvs/NLP-Classifier-6-qAEZNCR/bin/activate

    parser = argparse.ArgumentParser()
    parser.add_argument('--datadir', default='./data')
    parser.add_argument('--datafile', type=str, default='2019-11-11_matches.csv')
    parser.add_argument('--trainingdir', type=str, default='./updated_model_inputs')
    parser.add_argument('--combinedir', type=str, default='./combined_training')

    args = parser.parse_args()

    if not args.datafile:
        print("Input filename not supplied. Try again using '--datafile <filename>'")
        sys.exit()

    parser.add_argument('--outputfile', default=os.path.splitext(args.datafile)[0] + '_classification'
                                                + os.path.splitext(args.datafile)[1] , type=str)

    args = parser.parse_args()

    dfx = pd.read_csv(os.path.join(args.datadir,args.datafile))

    connect_to_orgclassifier()

    dfx['label'] = ''
    dfx['label'] = dfx.apply(classify_org, axis=1)
    dfx.to_csv(os.path.join(args.datadir, args.outputfile), index=False)

    combine_training_files(args)

    filtercombinedfileforunseenstrings(dfx, args)

    # pdb.set_trace()
    # dfx = pd.read_csv('./data/matches_labeled.csv')
    # df_train, df_valid, df_test = loadsplitdata(dfx)
    # Define the set of learning functions
    # lfs = [lf_regex_check_out]
    # lfs = [lf_iscompany]

    # Apply the LFs to the unlabeled training data
    # applier = PandasLFApplier(lfs)
    #
    # L_train = applier.apply(df_train)
    # L_valid = applier.apply(df_valid)

    # ANALYSIS SECTION
    # coverage_iscompany  = (L_train != ABSTAIN).mean(axis=0)
    # print(f"check_out coverage: {coverage_iscompany * 100:.1f}%")
    # print(f"check coverage: {coverage_iscompany * 100:.1f}%")
    #
    # LFAnalysis(L=L_train, lfs=lfs).lf_summary()
    # END ANALYSIS SECTION

    # Here we would seek to convert labels from multiple LFs into a 'noise-aware' single probabilistic label as to
    # whether or not each row is a company or not. We only have one LF, so we don't need this bit.i.e.:
    # from snorkel.labeling import MajorityLabelVoter
    #
    # majority_model = MajorityLabelVoter()
    # preds_train = majority_model.predict(L=L_train)

    # Train the label model and compute the training labels
    # pdb.set_trace()
    # label_model = LabelModel(cardinality=2, verbose=True)
    # label_model.fit(L_train, n_epochs=500, log_freq=50, seed=123)
    # df_train["label"] = label_model.predict(L=L_train, tie_break_policy="abstain")
    # df_train = df_train[df_train.label != ABSTAIN]

    # POSSIBLE DATA AUGMENTATION SECTION
    # transformation functions to expand the amount of data (replacing words with synonyms etc)
    # END DATA AUGMENTATION SECTION

    # TRAIN CLASSIFIER SECTION




    # FROM CLASSIFY.PY IN REALPYTHON TUTORIAL FOLDER
    # BUT THIS DEFINES A PRE-SET LABEL COLUMN FIRST AND MAKES THE SPLITS STRAIGHT AWAY
    # PROBABLY BETTER TO FOLLOW THE SPAM TUTORIAL METHODS

    # vectorizer = CountVectorizer()
    # vectorizer.fit(df_train)
    #
    # X_train = vectorizer.transform(df_train)
    # X_test = vectorizer.transform(df_test)
    #
    # ## NEED TO DEFINE Y_TRAIN AND Y_TEST (AKA GET A GOLD LABEL SET BY RUNNING THROUGH THE ORGTYPE CLASSIFIER
    #
    # classifier = LogisticRegression()
    # classifier.fit(X_train, y_train)
    # score = classifier.score(X_test, y_test)
    # print('Accuracy for {} data: {:.4f}'.format(source, score))
    #
    # input_dim = X_train.shape[1]  # Number of features
    #
    # model = Sequential()
    # model.add(layers.Dense(10, input_dim=input_dim, activation='relu'))
    # model.add(layers.Dense(1, activation='sigmoid'))
    #
    # model.compile(loss='binary_crossentropy',
    #               optimizer='adam',
    #               metrics=['accuracy'])
    # model.summary()
    #
    # history = model.fit(X_train, y_train,
    #                     epochs=100,
    #                     verbose=False,
    #                     validation_data=(X_test, y_test),
    #                     batch_size=10)
    #
    # loss, accuracy = model.evaluate(X_train, y_train, verbose=False)
    # print("Training Accuracy: {:.4f}".format(accuracy))
    # loss, accuracy = model.evaluate(X_test, y_test, verbose=False)
    # print("Testing Accuracy:  {:.4f}".format(accuracy))
    #
    # plt.style.use('ggplot')
    #
    # def plot_history(history):
    #     acc = history.history['acc']
    #     val_acc = history.history['val_acc']
    #     loss = history.history['loss']
    #     val_loss = history.history['val_loss']
    #     x = range(1, len(acc) + 1)
    #
    #     plt.figure(figsize=(12, 5))
    #     plt.subplot(1, 2, 1)
    #     plt.plot(x, acc, 'b', label='Training acc')
    #     plt.plot(x, val_acc, 'r', label='Validation acc')
    #     plt.title('Training and validation accuracy')
    #     plt.legend()
    #     plt.subplot(1, 2, 2)
    #     plt.plot(x, loss, 'b', label='Training loss')
    #     plt.plot(x, val_loss, 'r', label='Validation loss')
    #     plt.title('Training and validation loss')
    #     plt.legend()
    #
    # plot_history(history)
    #
    # tokenizer = Tokenizer(num_words=5000)
    # tokenizer.fit_on_texts(sentences_train)
    #
    # X_train = tokenizer.texts_to_sequences(sentences_train)
    # X_test = tokenizer.texts_to_sequences(sentences_test)
    #
    # vocab_size = len(tokenizer.word_index) + 1  # Adding 1 because of reserved 0 index
    #
    # print(sentences_train[2])
    # print(X_train[2])


if __name__ == '__main__':
    main()

