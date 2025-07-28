const crypto = require("crypto");
var path = require('path');
var mkdirp = require('mkdirp');
var { createFileNode } = require('gatsby-source-filesystem/create-file-node');
const { BlobServiceClient } = require("@azure/storage-blob");

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

async function downloadBlobFile(createNode, createNodeId, blobService, { container, name, localPath }) {
  mkdirp.sync(path.dirname(localPath));
  try {
    const containerClient = blobService.getContainerClient(container);
    const blockBlobClient = containerClient.getBlockBlobClient(name);
    await blockBlobClient.downloadToFile(localPath);

    const node = await createFileNode(localPath, createNodeId, {
      name: "gatsby-source-azure-storage"
    });

    const publicUrl = blockBlobClient.url;
    const nodeWithUrl = Object.assign({ url: publicUrl }, node);
    createNode(nodeWithUrl);

    return;
  } catch (err) {
    console.error(` Failed to download blob "${name}" from container "${container}"`);
    throw err;
  }
}

async function makeNodesFromContainer(createNode, createNodeId, containerClient, containerName, downloadFolder) {
  try {
    let nodes = [];
    for await (const blob of containerClient.listBlobsFlat()) {
      const item = {
        name: blob.name,
        container: containerName,
        contentMD5: blob.properties.contentMD5,
        creationTime: blob.properties.createdOn,
        lastModified: blob.properties.lastModified,
        blobType: blob.properties.blobType,
        serverEncrypted: blob.properties.serverEncrypted,
        localPath: downloadFolder ? path.join(process.cwd(), downloadFolder, blob.name) : null
      };

      const nodeId = createNodeId(`${item.name}/${blob.properties.contentMD5 || ''}`);
      const nodeContent = JSON.stringify(item);
      const nodeContentDigest = crypto
        .createHash('md5')
        .update(nodeContent)
        .digest('hex');
      const nodeData = Object.assign(item, {
        id: nodeId,
        parent: null,
        children: [],
        internal: {
          type: 'azureBlob',
          content: nodeContent,
          contentDigest: nodeContentDigest,
        },
      });
      createNode(nodeData);

      nodes.push(nodeData);
    }
    return nodes;
  } catch (err) {
    console.error(` Error on container "${containerName}"`);
    throw err;
  }
}

exports.sourceNodes = (
  { actions, createNodeId },
  configOptions
) => {
  const { createNode } = actions

  // Gatsby adds a configOption that's not needed for this plugin, delete it
  delete configOptions.plugins

  const blobService = BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING);

  let blobPromises = (configOptions.containers != null && configOptions.containers.length > 0)
    ? configOptions.containers.map(x => {
      let localFolder = (x.localFolder || configOptions.containerLocalFolder)
      makeContainerNode(createNode, createNodeId, x.name, localFolder)

      let containerClient = blobService.getContainerClient(x.name);
      let promiseNode = makeNodesFromContainer(createNode, createNodeId, containerClient, x.name, localFolder)

      if (localFolder == null) {
        return promiseNode
      } else {
        return promiseNode
          .then(values => {
            return Promise.all(values.map(node => {
              return downloadBlobFile(createNode, createNodeId, blobService, node)
            }))
          })
      }
    })
    : []

  return Promise.all(blobPromises)
}