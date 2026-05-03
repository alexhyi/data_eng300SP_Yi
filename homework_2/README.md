# Homework 2: Movie Recommendation System

This folder contains the source code for homework 2. 

The primary goal of this homework was to create a movie recommendation system using BERT and AWS. 

## Notebook Structure

The notebook is meant to be able to be run sequentially. 

1. The first 3 cells are installing and importing the necessary packages.
2. The next 4 cells define the functions to:
   * **2a.** Download the dataset into the S3 bucket
   * **2b.** Create the BERT embeddings for the movies
   * **2c.** Generate movie recommendations
   * **2d.** Present the recommended movies
3. The remaining cells call the functions while defining the necessary parameters, like naming the output files and the desired dataset.
