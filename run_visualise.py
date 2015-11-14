import matplotlib
matplotlib.use('Qt4Agg')
from matplotlib.pyplot import *
import smopy

box = (60.1442, 24.6351, 60.3190, 25.1741)
uni = [60.186228, 24.830285]
uni = [60.173423, 24.860154]
#uni = [60.173338, 24.860669]

def coord2pixels(x,y,lx,ly):
    px = (x - box[0]) / (box[2] - box[0])
    py = (y - box[1]) / (box[3] - box[1])
    print x, y
    print px, py
    print lx, ly
    return px * lx, py * ly

'''
img = imread('Helsinki.png')
print img.shape
#img=mpimg.imread('Helsinki.png')
iplt = imshow(img) #, extent=EXTENT)
xlim([0,img.shape[1]])
ylim([img.shape[0],0])
#fig = figure()
#ax = fig.add_subplot(121)
print img.shape
#map = smopy.Map(box,z=12)
uni = [60.173338, 24.860669]
x, y = coord2pixels(uni[0], uni[1], img.shape[1], img.shape[0])
print x, y
y = img.shape[0] - y

#x, y = map.to_pixels(60.20, 24.88)
#print x,y
#ax = map.show_mpl(figsize=(8, 6))
plot(x, y, 'or', markersize=10);
#map.show_ipython()
show()
exit(1)
'''

def get_trace(uni):
    map = smopy.Map(box,z=12)
    ax = map.show_mpl(figsize=(8, 6))
    map.show_ipython()

    for i in range(10):
        #uni = data[i,:]
    #uni = [60.187252, 24.827796]
        x, y = map.to_pixels(uni[0], uni[1])
        print x,y
        ax.plot(x, y, 'or', markersize=10);
        #plot(x, y, 'or', markersize=10);

    #from IPython.display import Image
    map.save_png('Helsinki2.png')
    #Image('Helsinki2.png')
    show()

def plot_trace(uni):
    img = imread('Helsinki.png')
    print img.shape
    #img=mpimg.imread('Helsinki.png')
    iplt = imshow(img) #, extent=EXTENT)
    xlim([0,img.shape[1]])
    ylim([img.shape[0],0])
    #fig = figure()
    #ax = fig.add_subplot(121)
    print img.shape
    #map = smopy.Map(box,z=12)
    x, y = coord2pixels(uni[0], uni[1], img.shape[1], img.shape[0])
    x = x + 210.
    print x, y
    y = img.shape[0] - y

    #x, y = map.to_pixels(60.20, 24.88)
    #print x,y
    #ax = map.show_mpl(figsize=(8, 6))
    plot(x, y, 'or', markersize=10);
    #map.show_ipython()
    show()

if __name__ == '__main__':
    get_trace(uni)
    #plot_trace(uni)

