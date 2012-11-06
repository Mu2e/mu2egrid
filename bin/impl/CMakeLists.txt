
INSTALL ( PROGRAMS mu2eart.sh
                   mu2eg4bl.sh
                   mu2emars.sh
          DESTINATION ${product}/${version}/bin/impl )

INSTALL ( FILES funcs
          DESTINATION ${product}/${version}/bin/impl )
