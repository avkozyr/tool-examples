import chalk from "chalk";
import StoryblokClient, { ISbStoryData, ISbResult } from "storyblok-js-client";
import FormData from "form-data";
import https from "https";
import fs from "fs";
import async from "async";
import { imageSizeFromFile } from "image-size/fromFile";

interface SbFolder {
  id: number;
  parent_id: number;
  name: string;
}

interface Asset {
  originalUrl: string;
  newUrl: string;
  originalId: number;
  newId?: number;
}

interface SbAsset {
  id: string;
  filename: string;
  asset_folder_id: number;
}

interface SbAssetData {
  asset_folder_id: number;
}

interface LocalAssetData {
  filename: string | undefined;
  folder: string;
  filepath: string;
  ext: string | undefined;
  size: string;
}

export default class Migration {
  sourceSpaceId: number;
  targetSpaceId: number;
  oauth: string;
  simultaneousUploads: number;
  sourceRegion: string;
  targetRegion: string;
  targetAssetsFolders: SbFolder[];
  sourceAssetsFolders: SbFolder[];
  assetsRetries: Record<string, number>;
  failedAssets: Asset[];
  sourceAssetsFoldersMap: Record<number, number>;
  retriesLimit: number;
  detectImageSize: boolean;
  clearSource: boolean;
  usedAssets: boolean;
  duplicateFolders: boolean;
  mapiClient: StoryblokClient;
  targetMapiClient: StoryblokClient;
  cdnApiClient?: StoryblokClient;
  stepsTotal: number;
  targetSpaceToken: string;
  storiesList: ISbStoryData[];
  updatedStories: ISbStoryData[];
  stringifiedStoriesList: string;
  assetsList: SbAsset[];
  targetAssetsList: SbAsset[];
  unusedAssetsList: SbAsset[];
  foldersToCreate: SbFolder[];
  assets: Asset[];
  limit: number;
  offset: number;
  importAssetsByIds: string[];

  constructor(
    oauth: string,
    sourceSpaceId: number,
    targetSpaceId: number,
    simultaneousUploads: number,
    sourceRegion: string,
    targetRegion: string,
    clearSource: boolean,
    detectImageSize: boolean,
    usedAssets: boolean,
    duplicateFolders: boolean,
    limit: number,
    offset: number,
    assetsIds: string[]
  ) {
    this.targetSpaceToken = "";
    this.storiesList = [];
    this.updatedStories = [];
    this.stringifiedStoriesList = "";
    this.assetsList = [];
    this.targetAssetsList = [];
    this.unusedAssetsList = [];
    this.foldersToCreate = [];
    this.assets = [];
    this.sourceSpaceId = sourceSpaceId;
    this.targetSpaceId = targetSpaceId;
    this.oauth = oauth;
    this.simultaneousUploads = simultaneousUploads || 5;
    this.sourceRegion = (sourceRegion || "eu").toLowerCase();
    this.targetRegion = (targetRegion || "eu").toLowerCase();
    this.sourceAssetsFolders = [];
    this.targetAssetsFolders = [];
    this.assetsRetries = {};
    this.sourceAssetsFoldersMap = {};
    this.retriesLimit = 15;
    this.detectImageSize = detectImageSize;
    this.clearSource = clearSource;
    this.usedAssets = usedAssets;
    this.failedAssets = [];
    this.duplicateFolders = duplicateFolders;
    this.limit = limit;
    this.offset = offset;
    this.importAssetsByIds = assetsIds;
    this.mapiClient = new StoryblokClient({
      oauthToken: this.oauth,
      region: this.sourceRegion,
      rateLimit: 3,
    });
    this.targetMapiClient =
      this.sourceRegion === this.targetRegion
        ? this.mapiClient
        : new StoryblokClient({
            oauthToken: this.oauth,
            region: this.targetRegion,
            rateLimit: 3,
          });
    this.stepsTotal = this.clearSource ? 9 : 8;
  }

  /**
   * Migration error callback
   */
  migrationError(err: string) {
    throw new Error(err);
  }

  /**
   * Check if an error is a rate limit (429) error
   */
  isRateLimitError(err: any): boolean {
    return (
      err?.status === 429 ||
      err?.response?.status === 429 ||
      err?.code === "ECONNABORTED" ||
      (err?.message && err.message.includes("429"))
    );
  }

  /**
   * Execute requests in batches with delay between batches and retry on 429
   */
  async batchRequests<T>(
    requests: (() => Promise<T>)[],
    batchSize: number = 3,
    delayMs: number = 350
  ): Promise<(T | null)[]> {
    const results: (T | null)[] = new Array(requests.length).fill(null);
    const maxRetries = 3;

    for (let i = 0; i < requests.length; i += batchSize) {
      const batch = requests.slice(i, i + batchSize);
      const batchIndices = batch.map((_, idx) => i + idx);

      const batchResults = await Promise.allSettled(batch.map((fn) => fn()));

      // Process results and collect failed indices for retry
      const toRetry: { index: number; request: () => Promise<T> }[] = [];

      batchResults.forEach((result, idx) => {
        const originalIndex = batchIndices[idx];
        if (result.status === "fulfilled") {
          results[originalIndex] = result.value;
        } else if (this.isRateLimitError(result.reason)) {
          toRetry.push({ index: originalIndex, request: requests[originalIndex] });
        } else {
          console.warn(`Request ${originalIndex} failed:`, result.reason?.message || result.reason);
          results[originalIndex] = null;
        }
      });

      // Retry failed 429 requests with exponential backoff
      for (const { index, request } of toRetry) {
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
          const waitTime = 1000 * Math.pow(2, attempt); // 2s, 4s, 8s
          await new Promise((r) => setTimeout(r, waitTime));
          try {
            results[index] = await request();
            break;
          } catch (retryErr: any) {
            if (attempt === maxRetries) {
              console.warn(`Request ${index} failed after ${maxRetries} retries`);
              results[index] = null;
            }
          }
        }
      }

      if (i + batchSize < requests.length) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
    return results;
  }

  /**
   * Print a message of the current step
   */
  stepMessage(index: string, text: string, append_text?: string) {
    process.stdout.clearLine(0);
    process.stdout.cursorTo(0);
    process.stdout.write(
      `${chalk.white.bgBlue(` ${index}/${this.stepsTotal} `)} ${text} ${
        append_text ? chalk.black.bgYellow(` ${append_text} `) : ""
      }`
    );
  }

  /**
   * Print a message of the completed step
   */
  stepMessageEnd(index: string, text: string) {
    process.stdout.clearLine(0);
    process.stdout.cursorTo(0);
    process.stdout.write(
      `${chalk.black.bgGreen(` ${index}/${this.stepsTotal} `)} ${text}\n`
    );
  }

  /**
   * Start the migration
   */
  async start() {
    try {
      fs.rmSync("./temp", { recursive: true, force: true });
      fs.mkdirSync("./temp");
      await this.getTargetSpaceToken();
      await this.getStories();
      await this.getAssetsFolders();
      await this.getAssets();
      await this.getTargetAssets();
      await this.createAssetsFolders();
      await this.uploadAssets();
      this.replaceAssetsInStories();
      await this.saveStories();
    } catch (err: any) {
      console.log(
        `${chalk.white.bgRed(` ⚠ Migration Error `)} ${chalk.red(
          err.toString().replace("Error: ", "")
        )}`
      );
    }
  }

  /**
   * Get the target space token and setup the Storyblok js client
   */
  async getTargetSpaceToken() {
    try {
      const spaceRequest = await this.targetMapiClient.get(
        `spaces/${this.targetSpaceId}`
      );
      this.targetSpaceToken = spaceRequest.data.space.first_token;
      this.cdnApiClient = new StoryblokClient({
        accessToken: this.targetSpaceToken,
        region: this.targetRegion,
        rateLimit: 3,
      });
    } catch (err) {
      console.log(err);
      this.migrationError(
        "Error trying to retrieve the space token. Please double check the target space id and the OAUTH token."
      );
    }
  }

  /**
   * Get the Stories from the target space
   */
  async getStories() {
    this.stepMessage("1", `Fetching stories from target space.`);
    try {
      if (this.cdnApiClient) {
        const links = await this.cdnApiClient.getAll("cdn/links", {
          version: "draft",
          per_page: 25,
        });
        const storyRequests = links.map(
          (link) => () =>
            this.targetMapiClient.get(
              `spaces/${this.targetSpaceId}/stories/${link.id}`
            )
        );
        const storiesResponsesManagement = await this.batchRequests(
          storyRequests,
          3,
          350
        );
        // Filter out failed requests (null values)
        this.storiesList = storiesResponsesManagement
          .filter((r): r is NonNullable<typeof r> => r !== null)
          .map((r) => r.data.story);
        this.stringifiedStoriesList = JSON.stringify(this.storiesList);
        const skipped = storyRequests.length - this.storiesList.length;
        if (skipped > 0) {
          console.log(`\nWarning: ${skipped} stories failed to fetch`);
        }
        this.stepMessageEnd("1", `Stories fetched from target space.`);
      } else {
        this.migrationError("The CDN API client is not initialized");
      }
    } catch (err) {
      console.log(err);
      this.migrationError(
        "Error fetching the stories. Please double check the target space id."
      );
    }
  }

  /**
   *
   * Map child folders recursively from source to target to prevent folders duplication
   */
  mapChildFolders(sourceFolder: SbFolder, targetFolder: SbFolder) {
    const sourceChildren = this.sourceAssetsFolders.filter(
      (folder) => folder.parent_id === targetFolder.id
    );
    const targetChildren = this.targetAssetsFolders.filter(
      (folder) => folder.parent_id === sourceFolder.id
    );
    targetChildren.forEach((targetChildFolder) => {
      const sourceChildFolder = sourceChildren?.find(
        (folder) => folder.name === targetChildFolder.name
      );
      if (sourceChildFolder) {
        this.sourceAssetsFoldersMap[targetChildFolder.id] =
          sourceChildFolder.id;
        this.mapChildFolders(sourceChildFolder, targetChildFolder);
      }
    });
  }

  /**
   * Get the Assets list from the source space
   */
  async getAssetsFolders() {
    this.stepMessage("2", `Fetching assets folders from source and target.`);
    try {
      const sourceAssetsFoldersRequest = await this.mapiClient.get(
        `spaces/${this.sourceSpaceId}/asset_folders`
      );
      this.sourceAssetsFolders = sourceAssetsFoldersRequest.data.asset_folders;
      // Prevent folders duplication
      if (!this.duplicateFolders) {
        const targetAssetsFoldersRequest = await this.targetMapiClient.get(
          `spaces/${this.targetSpaceId}/asset_folders`
        );
        this.targetAssetsFolders =
          targetAssetsFoldersRequest.data.asset_folders;
        const targetRootFolders = this.sourceAssetsFolders.filter(
          (f) => !f.parent_id
        );
        // Map source folders to target folders on the root level
        targetRootFolders.forEach((targetFolder) => {
          const sourceFolder = this.targetAssetsFolders.find(
            (sourceFolder) =>
              targetFolder.name === sourceFolder.name && !sourceFolder.parent_id
          );
          if (sourceFolder) {
            this.sourceAssetsFoldersMap[targetFolder.id] = sourceFolder.id;
            this.mapChildFolders(sourceFolder, targetFolder);
          }
        });
      }
      this.stepMessageEnd(
        "2",
        `Fetching assets folders from source and target.`
      );
    } catch (err) {
      this.migrationError(
        "Error fetching the assets folders. Please double check the source and target space IDs."
      );
    }
  }

  /**
   * Get the Assets list from the source space
   */
  async getAssets() {
    this.stepMessage("3", `Fetching assets from source space.`);
    let retrievedAssets = [];
    try {
      if (this.importAssetsByIds.length) {
        const assetsRequests = this.importAssetsByIds.map(
          (assetId) => () =>
            this.mapiClient.get(
              `spaces/${this.sourceSpaceId}/assets/${assetId}`
            )
        );
        const assetsResponses = await this.batchRequests(assetsRequests, 3, 350);
        // Filter out failed requests (null values)
        retrievedAssets = assetsResponses
          .filter((r): r is NonNullable<typeof r> => r !== null)
          .map((r) => r.data)
          .flat();
      } else {
        const assetsPageRequest = await this.mapiClient.get(
          `spaces/${this.sourceSpaceId}/assets`,
          {
            per_page: 100,
            page: 1,
          }
        );
        const assetsPageRequestTotal = assetsPageRequest.total || 1;
        // Calculating first page and last page based on VITE_OFFSET and VITE_LIMIT
        const firstPageIndex = Math.floor(this.offset / 100) + 1;
        const endPageLimit = Math.floor(this.limit / 100) + 1;
        const lastPageIndex =
          this.limit > 0
            ? endPageLimit
            : Math.ceil(assetsPageRequestTotal / 100);
        const assetsRequests: (() => Promise<ISbResult>)[] = [];
        for (let i = firstPageIndex; i <= lastPageIndex; i++) {
          const page = i;
          assetsRequests.push(() =>
            this.mapiClient.get(`spaces/${this.sourceSpaceId}/assets`, {
              per_page: 100,
              page,
            })
          );
        }
        const assetsResponses = await this.batchRequests(assetsRequests, 3, 350);
        // Filter out failed requests (null values)
        const assetsResponsesData = assetsResponses
          .filter((r): r is NonNullable<typeof r> => r !== null)
          .map((r) => r.data.assets)
          .flat();
        // Slicing the array in case VITE_OFFSET and VITE_LIMIT are set
        const sliceStart = this.offset % 100;
        const sliceEnd = sliceStart + this.limit;
        retrievedAssets =
          this.limit > 0
            ? assetsResponsesData.slice(sliceStart, sliceEnd)
            : assetsResponsesData.slice(sliceStart);
      }
      this.assetsList = retrievedAssets.map((asset) => {
        delete asset.space_id;
        delete asset.created_at;
        delete asset.updated_at;
        delete asset.published_at;
        delete asset.deleted_at;
        return asset;
      });
      if (this.usedAssets) {
        this.assetsList = this.assetsList.filter((asset) => {
          const filename = this.getAssetFilename(asset.filename);
          return this.stringifiedStoriesList.indexOf(filename) !== -1;
        });
      }
      this.stepMessageEnd("3", `Fetched assets from source space.`);
    } catch (err) {
      console.log(err);
      this.migrationError(
        "Error fetching the assets. Please double check the source space id."
      );
    }
  }

  /**
   * Get existing assets from target space for deduplication
   */
  async getTargetAssets() {
    this.stepMessage("4", `Fetching existing assets from target space.`);
    try {
      const assetsPageRequest = await this.targetMapiClient.get(
        `spaces/${this.targetSpaceId}/assets`,
        { per_page: 100, page: 1 }
      );
      const assetsTotal = assetsPageRequest.total || 1;
      const totalPages = Math.ceil(assetsTotal / 100);

      const assetsRequests: (() => Promise<ISbResult>)[] = [];
      for (let i = 1; i <= totalPages; i++) {
        const page = i;
        assetsRequests.push(() =>
          this.targetMapiClient.get(`spaces/${this.targetSpaceId}/assets`, {
            per_page: 100,
            page,
          })
        );
      }

      const assetsResponses = await this.batchRequests(assetsRequests, 3, 350);
      this.targetAssetsList = assetsResponses
        .filter((r): r is NonNullable<typeof r> => r !== null)
        .map((r) => r.data.assets)
        .flat();

      this.stepMessageEnd(
        "4",
        `Fetched ${this.targetAssetsList.length} existing assets from target space.`
      );
    } catch (err) {
      console.log(err);
      // Non-fatal: continue with empty list (will upload all assets)
      console.log("Warning: Could not fetch target assets for deduplication");
      this.targetAssetsList = [];
    }
  }

  /**
   * Check if a folder and its ancestors are not orphans
   */
  folderNotOrphan(folder: SbFolder): boolean {
    const visited = new Set<number>();
    let current = folder;

    while (current.parent_id) {
      if (visited.has(current.id)) {
        console.warn(
          `Circular folder reference detected at folder ID ${current.id}`
        );
        return false; // circular detected → treat as orphan
      }
      visited.add(current.id);

      const parent = this.sourceAssetsFolders.find(
        (f) => f.id === current.parent_id
      );
      if (!parent) return false; // missing parent → orphan

      current = parent;
    }

    return true; // reached root without loops
  }

  /**
   * Get the Assets list from the source space
   */
  /**
   * Create assets folders in target space (safe with circular check)
   */
  async createAssetsFolders() {
    this.stepMessage("5", `Creating assets folders in target space.`);
    try {
      // filter folders that need creation and are not orphans
      const foldersToCreate = this.sourceAssetsFolders.filter(
        (f) => !this.sourceAssetsFoldersMap[f.id] && this.folderNotOrphan(f)
      );

      // create folders in target space
      for (let index = 0; index < foldersToCreate.length; index++) {
        const currentFolder = foldersToCreate[index];
        const folderResponse = (await this.targetMapiClient.post(
          `spaces/${this.targetSpaceId}/asset_folders`,
          { name: currentFolder.name }
        )) as any;

        this.sourceAssetsFoldersMap[currentFolder.id] =
          folderResponse.data.asset_folder.id;
        this.targetAssetsFolders.push(folderResponse.data.asset_folder);
      }

      // update parent_id references based on the mapping
      foldersToCreate.forEach((folder) => {
        if (folder.parent_id && this.sourceAssetsFoldersMap[folder.parent_id]) {
          folder.parent_id = this.sourceAssetsFoldersMap[folder.parent_id];
        }
      });

      this.foldersToCreate = foldersToCreate;

      // set parent_id in target space for folders with a parent
      const foldersWithParent = foldersToCreate.filter((f) => f.parent_id);
      for (let index = 0; index < foldersWithParent.length; index++) {
        const currentFolder = foldersWithParent[index];
        const currentFolderId = this.sourceAssetsFoldersMap[currentFolder.id];

        await this.targetMapiClient.put(
          `spaces/${this.targetSpaceId}/asset_folders/${currentFolderId}`,
          { parent_id: currentFolder.parent_id }
        );
      }

      // update folder IDs for assets
      this.assetsList.forEach((asset) => {
        asset.asset_folder_id =
          this.sourceAssetsFoldersMap[asset.asset_folder_id];
      });

      this.stepMessageEnd("5", `Assets folders created in target space`);
    } catch (err) {
      console.log(err);
      this.migrationError(
        "Error creating the assets folders. Please double check the source space id."
      );
    }
  }

  /**
   *
   * Return the clean filename of an asset without the S3 bucket reference
   */
  getAssetFilename(filename: string) {
    try {
      return `/${filename.slice(
        filename.search("/a(-[a-z]+)?.storyblok.com/")
      )}`;
    } catch (err: any) {
      return typeof filename === "string" ? filename : "";
    }
  }

  /**
   * Upload all assets to the target space with concurrency and iterative retries
   */
  async uploadAssets() {
    // Deduplication: filter out assets that already exist in target
    const targetAssetFilenames = new Set(
      this.targetAssetsList.map((a) => this.getAssetFilename(a.filename))
    );
    const assetsToUpload = this.assetsList.filter(
      (asset) => !targetAssetFilenames.has(this.getAssetFilename(asset.filename))
    );
    const skippedCount = this.assetsList.length - assetsToUpload.length;
    if (skippedCount > 0) {
      console.log(`\nSkipping ${skippedCount} assets (already exist in target)`);
    }

    this.stepMessage("6", ``, `0 of ${assetsToUpload.length} assets to upload`);
    this.assets = [];

    let totalUploaded = 0;

    // async.eachLimit for concurrency
    await new Promise<void>((resolve) => {
      async.eachLimit(
        assetsToUpload,
        this.simultaneousUploads,
        async (asset: SbAsset) => {
          const assetUrl = this.getAssetFilename(asset.filename);
          const assetData = JSON.parse(JSON.stringify(asset));
          delete assetData.filename;

          // track original asset
          this.assets.push({
            originalUrl: assetUrl,
            newUrl: `https:${assetUrl}`,
            originalId: assetData.id,
          });
          delete assetData.id;

          const success = await this.uploadAsset(assetUrl, assetData);

          // Small delay between uploads to avoid rate limits
          await new Promise((r) => setTimeout(r, 200));

          totalUploaded++;
          this.stepMessage(
            "6",
            ``,
            `${totalUploaded} of ${assetsToUpload.length} assets uploaded${
              success ? "" : " (failed)"
            }`
          );
        },
        () => {
          process.stdout.clearLine(0);
          this.stepMessageEnd(
            "6",
            `Uploaded ${totalUploaded} of ${assetsToUpload.length} assets to target space.`
          );
          resolve();
        }
      );
    });
  }

  /**
   * Return an object with filename, folder and filepath of an asset in the temp folder
   */
  getLocalAssetData(url: string): LocalAssetData {
    const rootRegex = /\/\/a(-[a-z]+)?.storyblok.com\/f\//i;
    const urlParts = url.replace(rootRegex, "").split("/");
    const ext = url
      .split("?")[0]
      .split("/")
      .pop()
      ?.split(".")
      ?.pop()
      ?.toLowerCase();
    const size = urlParts.length === 4 ? urlParts[1] : "";

    return {
      filename: url.split("?")[0].split("/").pop(),
      folder: `./temp/${url.split("?")[0].split("/").slice(0, -1).pop()}`,
      filepath: `./temp/${url.split("?")[0].split("/").slice(0, -1).pop()}/${url
        .split("?")[0]
        .split("/")
        .pop()}`,
      ext,
      size,
    };
  }

  /**
   * Download an asset and store it into the temp folder
   */
  async downloadAsset(url: string) {
    const localAssetData = this.getLocalAssetData(url);
    if (!fs.existsSync(localAssetData.folder)) {
      fs.mkdirSync(localAssetData.folder);
    }
    const file = fs.createWriteStream(localAssetData.filepath);
    return new Promise((resolve, reject) => {
      https
        .get(`https:${url}`, (res) => {
          res.pipe(file);
          file.on("finish", function () {
            file.close();
            resolve(true);
          });
        })
        .on("error", (err) => {
          reject(err);
        });
    });
  }

  /**
   * Upload a single asset to the space with iterative retries
   */
  async uploadAsset(
    assetUrl: string,
    storyblokAssetData: SbAssetData
  ): Promise<boolean> {
    const maxRetries = this.retriesLimit;
    let attempt = this.assetsRetries[assetUrl] || 0;

    while (attempt <= maxRetries) {
      try {
        const localAssetData = this.getLocalAssetData(assetUrl);
        let size = localAssetData.size;

        // download asset
        await this.downloadAsset(assetUrl);

        // detect image dimensions if needed
        if (
          this.detectImageSize &&
          !size &&
          localAssetData.ext &&
          ["jpg", "jpeg", "gif", "png", "webp", "avif", "svg"].includes(
            localAssetData.ext
          )
        ) {
          try {
            const dimensions = await imageSizeFromFile(localAssetData.filepath);
            size = `${dimensions.width}x${dimensions.height}`;
          } catch (err) {
            console.log(
              `Error getting dimensions for ${localAssetData.filename}:`,
              err
            );
            size = "";
          }
        }

        if (size === "x") size = "";

        const newAssetPayload = {
          ...storyblokAssetData,
          filename: assetUrl,
          ...(size && { size }),
        };

        const newAssetRequest = (await this.targetMapiClient.post(
          `spaces/${this.targetSpaceId}/assets`,
          newAssetPayload
        )) as any;

        if (newAssetRequest.status !== 200) return true;

        const signedRequest = newAssetRequest.data;
        const form = new FormData();
        for (const key in signedRequest.fields) {
          form.append(key, signedRequest.fields[key]);
        }
        form.append("file", fs.createReadStream(localAssetData.filepath));

        await new Promise<void>((resolve, reject) => {
          form.submit(signedRequest.post_url, (err) => {
            // cleanup temp files
            if (
              fs.existsSync(localAssetData.filepath) ||
              fs.existsSync(localAssetData.folder)
            ) {
              fs.rmSync(localAssetData.folder, {
                recursive: true,
                force: true,
              });
            }

            if (err) return reject(err);

            const assetObject = this.assets.find(
              (item) => item && item.originalUrl === assetUrl
            );
            if (assetObject) {
              assetObject.newUrl = `https:${signedRequest.pretty_url}`;
              assetObject.newId = signedRequest.id;

              this.targetMapiClient
                .get(
                  `spaces/${this.targetSpaceId}/assets/${signedRequest.id}/finish_upload`
                )
                .then(() => resolve())
                .catch(() => reject());
            } else {
              resolve();
            }
          });
        });

        // success → reset retry counter
        this.assetsRetries[assetUrl] = 0;
        return true;
      } catch (err: any) {
        // check if retryable error
        const retryable = this.isRateLimitError(err);

        if (!retryable || attempt === maxRetries) {
          const assetObject = this.assets.find(
            (item) => item && item.originalUrl === assetUrl
          );
          if (assetObject) this.failedAssets.push(assetObject);

          console.log(
            `Failed to upload asset ${assetUrl} after ${attempt} retries`,
            err
          );
          return false;
        }

        // increment retry counter and loop
        attempt++;
        this.assetsRetries[assetUrl] = attempt;

        const waitTime = 1000 * Math.pow(2, attempt); // exponential backoff
        console.log(
          `Retrying asset ${assetUrl} in ${
            waitTime / 1000
          }s (attempt ${attempt})`
        );
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      }
    }

    return false;
  }

  /**
   * Replace the asset's id and filename in the tree of a story
   */
  replaceAssetInData(data: any, asset: Asset): any {
    if (Array.isArray(data)) {
      return data.map((item) => this.replaceAssetInData(item, asset));
    } else if (
      data &&
      typeof data === "object" &&
      this.getAssetFilename(data.filename) === asset.originalUrl &&
      data.id === asset.originalId
    ) {
      return { ...data, id: asset.newId, filename: asset.newUrl };
    } else if (data && typeof data === "object") {
      return Object.keys(data).reduce(
        (newObject: Record<string, any>, key: string) => {
          const propertyValue = this.replaceAssetInData(data[key], asset);
          newObject[key] = propertyValue;
          return newObject;
        },
        {}
      );
    } else if (
      data &&
      typeof data === "string" &&
      this.getAssetFilename(data) === asset.originalUrl
    ) {
      return asset.newUrl;
    }
    return data;
  }

  /**
   * Replace the new urls in the target space stories
   */
  replaceAssetsInStories() {
    this.stepMessage("7", ``, `0 of ${this.assets.length} URLs replaced`);
    this.updatedStories = this.storiesList.slice(0);
    this.assets.forEach((asset, index) => {
      this.updatedStories = this.replaceAssetInData(this.updatedStories, asset);
      this.stepMessage(
        "7",
        ``,
        `${index} of ${this.assets.length} URLs replaced`
      );
    });
    this.stepMessageEnd("7", `Replaced all URLs in the stories.`);
  }

  /**
   * Save the updated stories in Storyblok
   */
  async saveStories() {
    let total = 0;
    const storiesWithUpdates = this.updatedStories.filter((story) => {
      const originalStory = this.storiesList.find((s) => s.id === story.id);
      return originalStory
        ? JSON.stringify(originalStory.content) !==
            JSON.stringify(story.content)
        : true;
    });

    const migrationResult = await Promise.allSettled(
      storiesWithUpdates.map(async (story) => {
        delete story.content._editable;
        const post_data: any = { story };
        if (story.published && !story.unpublished_changes) {
          post_data.publish = 1;
        }
        try {
          await this.targetMapiClient.put(
            `spaces/${this.targetSpaceId}/stories/${story.id}`,
            post_data
          );
          this.stepMessage(
            "8",
            ``,
            `${++total} of ${storiesWithUpdates.length} stories updated.`
          );
          return true;
        } catch (err) {
          return false;
        }
      })
    );
    process.stdout.clearLine(0);
    this.stepMessageEnd(
      "8",
      `Updated ${
        migrationResult.filter((r) => r.status === "fulfilled" && r.value)
          .length
      } ${
        migrationResult.filter((r) => r.status === "fulfilled" && r.value)
          .length === 1
          ? "story"
          : "stories"
      } in target space.`
    );
    fs.writeFileSync(
      "./log.json",
      JSON.stringify(
        {
          "updated-stories": storiesWithUpdates,
          "uploaded-assets": this.assetsList,
          "created-folders": this.foldersToCreate,
          "failed-assets": this.failedAssets,
        },
        null,
        4
      )
    );
  }
}