
NIFI_HOME=$1

if [ -z "$NIFI_HOME" -a "${NIFI_HOME+xxx}" = "xxx" ]; then
  echo "You must provide the NiFi home directory as an argument to this script."
  exit
fi

echo "Removing old nars from $NIFI_HOME/lib..."
rm $NIFI_HOME/lib/nifi-example-utils-nar*

echo "Deploying latest nars to $NIFI_HOME/lib..."
cp nifi-example-utils-bundle/nifi-example-utils-nar/target/nifi-example-utils-nar*.nar $NIFI_HOME/lib/

echo "DONE!"
