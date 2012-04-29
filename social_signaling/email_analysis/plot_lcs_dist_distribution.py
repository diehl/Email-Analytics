import marshal
import matplotlib.pyplot as plt

# Load the pairwise distances
f = open('subject_line_distances.txt','r')
dists = marshal.loads(f.read())

# Turn on interactive mode
plt.ion()

# Plot histogram with 1000 bins
plt.hist(dists,bins=1000)
paxes = plt.gca()
paxes.set_xlim([-0.05,1.05])
plt.xlabel('lcs_dist')
plt.ylabel('counts')
plt.title('lcs_dist distribution for 1000000 message subject line pairs')


