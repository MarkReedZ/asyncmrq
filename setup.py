from setuptools import setup
from mrq.client import __version__

setup(
    name='asyncmrq',
    version=__version__,
    description='MrQ client for Python Asyncio',
    long_description='Asyncio based Python client for MrQ, a ridiculously fast message queue',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
        ],
    url='https://github.com/MarkReedZ/asyncmrq',
    author='Mark Reed',
    author_email='mark@untilfluent.com',
    license='MIT License',
    packages=['asyncmrq'],
    zip_safe=True,
)
