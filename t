const fs = require("fs");
const path = require("path");
const axios = require("axios");

function getRenovateConfig(repoUrl) {
  const renovateUrl = `${repoUrl}/.github/renovate.json`;
  return axios.get(renovateUrl)
    .then(response => response.data)
    .catch(error => {
      console.error(`Error fetching Renovate configuration: ${error}`);
      return {};
    });
}

function getDetectedDependencies(repoUrl) {
  const renovateConfig = getRenovateConfig(repoUrl);
  const dependencies = {};
  return renovateConfig.then(config => {
    for (const tool of config.tools || []) {
      if (tool.name === "github-actions") {
        dependencies.githubActions = getGithubActionsDependencies(tool.config);
      } else if (tool.name === "pip_setup") {
        dependencies.pipSetup = getPipSetupDependencies(tool.config);
      }
    }
    return dependencies;
  });
}

function getGithubActionsDependencies(config) {
  const dependencies = [];
  for (const action of config.actions || []) {
    dependencies.push(action.name);
  }
  return dependencies;
}

function getPipSetupDependencies(config) {
  const dependencies = [];
  for (const package_ of config.packages || []) {
    dependencies.push(`${package_.name}==${package_.version}`);
  }
  return dependencies;
}

function printDetectedDependencies(repoUrl) {
  const dependencies = getDetectedDependencies(repoUrl);
  dependencies.then(dependencies => {
    for (const tool in dependencies) {
      console.log(`<details><summary>${tool} (${dependencies[tool].length})</summary>`);
      for (const dep of dependencies[tool]) {
        console.log(` - ${dep}`);
      }
      console.log("</details>");
    }
  });
}

function main() {
  const repoUrl = "https://github.com/IBM/nzpy";
  printDetectedDependencies(repoUrl);
}

main();