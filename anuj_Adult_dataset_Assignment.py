
# coding: utf-8

# In[105]:


#Adult Dataset Assignment
import numpy as np
import pandas as pd


# In[106]:


# csv file load without col names
adult_data_df = pd.read_csv("C:/Users/tewat/DataSciense/adult.data.txt",header=None)


# In[107]:


# without col names
adult_data_df.head()


# In[108]:


#variable types
adult_data_df.dtypes


# In[109]:


#with first two cols
adults_cols = ['age','workclass']


# In[110]:


adult_data_df = pd.read_csv("C:/Users/tewat/DataSciense/adult.data.txt",header=None, names=adults_cols, usecols=[0,1])


# In[111]:


adult_data_df.head()


# In[112]:


adults_cols = ['age','workclass','fnlwgt','education','education-num','marital-status','occupation','relationship','race','sex',
                  'capital-gain','capital-loss','hours-per-week','native-country']


# In[113]:


#with col names
adult_data_df = pd.read_csv("C:/Users/tewat/DataSciense/adult.data.txt", header=None, names=adults_cols, 
                            usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13])


# In[114]:


adult_data_df.head()


# In[115]:


adult_data_df.columns


# In[116]:


adult_data_df['age'].head()


# In[117]:


adult_data_df_age = adult_data_df['age'] > 25


# In[118]:


adult_data_df_age.value_counts()


# In[119]:


adult_data_df['age'].unique()


# In[120]:


adult_data_df['native-country'].value_counts()


# In[121]:


adult_data_df_age = adult_data_df[(adult_data_df['age']>35) & (adult_data_df['age']<53)]


# In[130]:


adult_data_df_age.head()


# In[140]:


adult_data_df[['age', 'sex', 'education', 'native-country']][:10]

