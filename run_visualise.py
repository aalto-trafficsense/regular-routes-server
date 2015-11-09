import matplotlib
matplotlib.use('Qt4Agg')
from matplotlib.pyplot import *
import smopy

box = (60.1442, 24.6351, 60.3190, 25.1741)

def coord2pixels(x,y,lx,ly):
    px = (x - box[0]) / (box[2] - box[0])
    py = (y - box[1]) / (box[3] - box[1])
    print x, y
    print px, py
    print lx, ly
    return int(px * lx), int(py * ly)

def plot_trace(data):
    map = smopy.Map(box,z=12)
    ax = map.show_mpl(figsize=(8, 6))
    map.show_ipython()

    for i in range(10):
        uni = data[i,:]
    #uni = [60.187252, 24.827796]
        x, y = map.to_pixels(uni[0], uni[1])
        print x,y
        ax.plot(x, y, 'or', markersize=10);
        plot(x, y, 'or', markersize=10);

    #from IPython.display import Image
    #map.save_png('testMap2.png')
    #Image('testMap2.png')
    show()

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
    '''

if __name__ == '__main__':
    plot_trace()

