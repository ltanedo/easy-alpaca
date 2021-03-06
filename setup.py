from distutils.core import setup
setup(
  name = 'easy_alpaca',         # How you named your package folder (MyLib)
  packages = ['easy_alpaca','easy_alpaca.__endpoints__','easy_alpaca.__utility__'],   # Chose the same as "name"
  version = '0.2',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'A library for the Alpaca Trading and Data API',   # Give a short description about your library
  author = 'lloydtan',                   # Type in your name
  author_email = 'lloydtan@buffalo.edu',      # Type in your E-Mail
  url = 'https://github.com/ltanedo/easy-alpaca.git',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/ltanedo/easy-alpaca/archive/refs/tags/v_02.tar.gz',    # I explain this later on
  keywords = ['python', 'algo', 'trading', 'alpaca', 'brokerage', 'stocks', 'stock'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.8',
  ],
)