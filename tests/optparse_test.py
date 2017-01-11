import optparse

parser = optparse.OptionParser()

parser.add_option('-t', '--test', action='store_true', dest='test', default=False)

opts, args = parser.parse_args()

print opts, args

