import colorsys
import logging


class Utils:
    """ Mongo Service class containing all methods for using mongo and python """

    def __init__(self):
        raise RuntimeError("Constructor cannot be called.")

    def get_n_colors(n=5):
        """ Generate a list on N color in output (for graph)"""
        logger = logging.getLogger("my logger")
        if n is None:
            logger.fatal("Error. value cant be null. Must be an Integer.")
            raise RuntimeError("Error. value cant be null. Must be an Integer.")
        hsv_tuples = [(x * 1.0 / n, 0.5, 0.5) for x in range(n)]
        hex_out = []
        for rgb in hsv_tuples:
            rgb = map(lambda x: int(x * 255), colorsys.hsv_to_rgb(*rgb))
            hex_out.append('#%02x%02x%02x' % tuple(rgb))
        return hex_out