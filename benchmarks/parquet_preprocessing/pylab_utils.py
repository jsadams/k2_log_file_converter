import os
import sys

def add_pylab_to_syspath():
    HOME = os.getenv('HOME')
    pylab_path=os.path.join(HOME,"usr","share","pylab")
    sys.path.append(pylab_path)
