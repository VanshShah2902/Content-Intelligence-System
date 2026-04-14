import sys
import os

# Add relevant path
sys.path.append(r"C:\Users\vansh\OneDrive\Documents\zj\Content_intelligence_engine\topic_intelligence_engine")

from clustering.clustering_service import cluster_posts
from ingestion.instagram import get_mock_posts

# Get the realistic mock posts we just updated
posts = get_mock_posts()

print(f"Clustering {len(posts)} posts...")
clusters = cluster_posts(posts)

for c in clusters:
    print(f"Cluster: {c['topic_name']} | Posts: {c['total_posts']} | Avg Weighted: {c['avg_weighted_score']}")
    for p in c['top_posts'][:2]:
        print(f"  - {p['text'][:50]}...")
