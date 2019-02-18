# Collaboration Distance Analysis of Arxiv Papers

```console
import arxiv
distance, ancestry = arxiv.dist("Peter Freeman", "Alessandro Rinaldo", max_depth = 3)
print(distance) # 2
print(ancestry) # [352, 1557, 1690], where Peter is 1690, Larry 1557 and Ale 352.
```
