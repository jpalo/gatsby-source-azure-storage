const crypto = require("crypto");
const { BlobServiceClient } = require('@azure/storage-blob');
var path = require('path');
var mkdirp = require('mkdirp');
const fs = require('fs');
var { createFileNode } = require('gatsby-source-filesystem/create-file-node');

const getValueWithDefault = (valueItem, defaultValue) => { return ((valueItem || { _: defaultValue })._ || defaultValue) }
const getValue = valueItem => getValueWithDefault(valueItem, null)

function makeContainerNode(createNode, createNodeId, containerName, localFolder) {
  const item = {
    name: containerName,
    localFolder: localFolder
  }
  const nodeId = createNodeId('azureContainer/' + containerName)
  const nodeContent = JSON.stringify(item)
  const nodeContentDigest = crypto
    .createHash('md5')
    .update(nodeContent)
    .digest('hex')
  const nodeData = Object.assign(item, {
    id: nodeId,
    parent: null,
    children: [],
    internal: {
      type: 'azureContainer',
      content: nodeContent,
      contentDigest: nodeContentDigest,
    },
  })
  createNode(nodeData)
}

async function downloadBlobFile(createNode, createNodeId, blobServiceClient, containerClient, { container, name, localPath }) {
  mkdirp.sync(path.dirname(localPath, createNodeId));

  try {
    let blockBlobClient = containerClient.getBlockBlobClient(name);

    const downloadBlockBlobResponse = await blockBlobClient.download(0);

    await streamToLocalFile(downloadBlockBlobResponse.readableStreamBody, localPath);
    createFileNode(localPath, createNodeId, pluginOptions = {
      name: "gatsby-source-azure-storage"
    })
    createFileNode(localPath, createNodeId, pluginOptions = {
      name: "gatsby-source-azure-storage"
    }).then(function (node) {
      let nodeWithUrl = Object.assign({ url: blockBlobClient.url }, node)
      createNode(nodeWithUrl)
    }, function (failure) {
      console.error(` Failed creating node from blob "${name}" from container "${container}"`)
    })

  } catch (err) {
    console.error(` Failed to download blob "${name}" from container "${container}"`)
  }
}

async function streamToLocalFile(readableStream, destination) {
  return new Promise((resolve, reject) => {
    let buffer = Buffer.from([]);
    readableStream.on("data", (data) => {
      buffer = Buffer.concat([buffer, data], buffer.length + data.length);//Add the data read to the existing buffer.
    });
    readableStream.on("end", () => {
      fs.writeFileSync(destination, buffer);//Write buffer to local file.
      resolve(destination);//Return that file path.  
    });
    readableStream.on("error", reject);
  });
}

async function makeNodesFromContainer(createNode, createNodeId, containerClient, containerName, downloadFolder) {
  try {
    var nodes = [];

    let blobs = await containerClient.listBlobsFlat();

    let blobItem = await blobs.next();
    while (!blobItem.done) {
      let value = blobItem.value;

      const item = {
        name: value.name,
        container: value.container || containerName,
        contentMD5: String.fromCharCode(...value.properties.contentMD5),
        creationTime: value.properties.createdOn,
        lastModified: value.properties.lastModified,
        blobType: value.properties.blobType,
        serverEncrypted: value.properties.serverEncrypted,
        localPath: (downloadFolder == null ? null : path.join(process.cwd(), downloadFolder, value.name))
      }
      const nodeId = createNodeId(`${value.name}/${item.contentMD5}`)
      const nodeContent = JSON.stringify(item)
      const nodeContentDigest = crypto
        .createHash('md5')
        .update(nodeContent)
        .digest('hex')
      const nodeData = Object.assign(item, {
        id: nodeId,
        parent: null,
        children: [],
        internal: {
          type: 'azureBlob',
          content: nodeContent,
          contentDigest: nodeContentDigest,
        },
      })
      createNode(nodeData)

      nodes.push(nodeData)
      blobItem = await blobs.next();
    }

  } catch (err) {
    console.error(` Error on container "${containerName}": ${err}`)
  }

  return nodes;
}

exports.sourceNodes = async (
  { actions, createNodeId },
  configOptions
) => {
  const { createNode } = actions

  // Gatsby adds a configOption that's not needed for this plugin, delete it
  delete configOptions.plugins
  
  try {
  console.log("-connstr-" + process.env.AZURE_STORAGE_CONNECTION_STRING + "---");
    
  const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING)

  let blobPromises = (configOptions.containers != null && configOptions.containers.length > 0)
    ? configOptions.containers.map(async (x) => {
      let localFolder = (x.localFolder || configOptions.containerLocalFolder)
      makeContainerNode(createNode, createNodeId, x.name, localFolder)

      const containerClient = blobServiceClient.getContainerClient(x.name);
      let promiseNode = makeNodesFromContainer(createNode, createNodeId, containerClient, x.name, localFolder)

      if (localFolder == null) {
        return promiseNode
      } else {
        return promiseNode
          .then(values => {
            return Promise.all(values.map(async (node) => {
              await downloadBlobFile(createNode, createNodeId, blobServiceClient, containerClient, node)
            }))
          })
      }
    })
    : []
  } catch(err) {
    console.error(err);
  }
  return await Promise.all(blobPromises)
}
