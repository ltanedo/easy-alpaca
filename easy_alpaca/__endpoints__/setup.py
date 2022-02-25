from distutils.core import setup
setup(
  name = 'easy_td',         # How you named your package folder (MyLib)
  packages = ['easy_td','easy_td.__endpoints__','easy_td.__utlity__'],   # Chose the same as "name"
  version = '0.2',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'A declarative library for making the TD Ameritrade API more accessible',   # Give a short description about your library
  author = 'lloydtan',                   # Type in your name
  author_email = 'lloydtan@buffalo.edu',      # Type in your E-Mail
  url = 'https://github.com/ltanedo/easy-td.git',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/ltanedo/easy-td/archive/refs/tags/v_02.tar.gz',    # I explain this later on
  keywords = ['python', 'algo', 'trading', 'data', 'stocks', 'stock','td','ameritrade'],   # Keywords that define your package best
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