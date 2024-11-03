from elasticsearch import Elasticsearch
import streamlit as st

client = Elasticsearch(st.secrets["es"]["url"], st.secrets["es"]["key"])
