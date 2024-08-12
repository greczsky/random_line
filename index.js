#!/usr/bin/env node

import { parseArgs } from "node:util";
import { createReadStream, readFileSync, existsSync, writeFile } from "node:fs";
import { createInterface } from "node:readline/promises";

const { positionals } = parseArgs({
  options: { filePath: { type: "string" }, lineIdx: { type: "string" } },
  allowPositionals: true,
});

const filePath = positionals[0];
const lineIdx = parseInt(positionals[1], 10);
let index = [0];

const randomLine = (path, idx) => {
  const indexFilePath = `${path}.idx`;
  //retrieve index from index file
  if (existsSync(indexFilePath)) {
    index = readIndexFromFile(indexFilePath);
    retrieveLine(path, idx);
  } else {
    // create index via reading the input file
    console.log(`Writing index to ${indexFilePath}...`);
    let offset = 0;
    // read stream because it's not possible to load 1TB of data to heap
    const readStream = createReadStream(path, {
      encoding: "utf-8",
    });
    let readLine = createInterface({ input: readStream });
    readLine.on("line", (line) => {
      offset += Buffer.byteLength(line, "utf8") + 1; // line length + length of \n is the offset of next line
      index.push(offset);
    });
    readLine.on("close", () => {
      console.log("done");
      retrieveLine(path, idx);
      writeIndexToFile(indexFilePath);
    });
    readLine.on("error", (error) => console.error(error.message));
  }
};

const retrieveLine = (path, idx) => {
  const readStream = createReadStream(path, {
    encoding: "utf-8",
    start: index[idx],
    end: index[idx + 1],
  });
  const readLine = createInterface({ input: readStream });
  // once => only one line
  readLine.once("line", (line) => {
    console.log(line);
  });
  readLine.on("error", (error) => console.error(error.message));
};

const readIndexFromFile = (path) => {
  return readFileSync(`${path}`, { encoding: "utf8" })
    .split(",")
    .map((offset) => parseInt(offset, 10));
};

const writeIndexToFile = (path) => {
  writeFile(`${path}`, index.join(","), (err) => {
    if (err) {
      console.error(err);
    }
  });
};

randomLine(filePath, lineIdx);
