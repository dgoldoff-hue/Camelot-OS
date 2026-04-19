/**
 * hubspot_deals.js — HubSpot Deal Pipeline Integration
 * Camelot Realty Group / Broker Bot
 *
 * Manages brokerage deal pipeline in HubSpot CRM.
 * Pipeline: "Camelot Brokerage"
 * Stages: Prospect → LOI Submitted → Under Contract → Closed / Dead
 *
 * Required env var: HUBSPOT_ACCESS_TOKEN
 *
 * Usage:
 *   const broker = require('./hubspot_deals');
 *   await broker.createBrokerageDeal(listingData);
 */

"use strict";

const https = require("https");
const { promisify } = require("util");

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const HUBSPOT_BASE_URL = "https://api.hubapi.com";
const ACCESS_TOKEN = process.env.HUBSPOT_ACCESS_TOKEN;

if (!ACCESS_TOKEN) {
  console.warn(
    "[hubspot_deals] WARNING: HUBSPOT_ACCESS_TOKEN is not set. All API calls will fail."
  );
}

// Pipeline and stage IDs — these must match your HubSpot account configuration.
// Run `listPipelineStages()` to print the actual IDs from your account.
const BROKERAGE_PIPELINE_NAME = "Camelot Brokerage";

// Stage name → internal key mapping (display names, used for lookup)
const STAGE_NAMES = {
  PROSPECT: "Prospect",
  LOI_SUBMITTED: "LOI Submitted",
  UNDER_CONTRACT: "Under Contract",
  CLOSED_WON: "Closed",
  CLOSED_LOST: "Dead",
};

// Cache for pipeline/stage ID lookup
let _pipelineCache = null;

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/**
 * Make a JSON API request to HubSpot.
 * @param {string} method - HTTP method (GET, POST, PATCH, etc.)
 * @param {string} path   - API path (e.g. "/crm/v3/objects/deals")
 * @param {object|null} body - Request body (will be JSON.stringify'd)
 * @returns {Promise<object>} Parsed JSON response
 */
async function hubspotRequest(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const bodyStr = body ? JSON.stringify(body) : null;

    const options = {
      hostname: "api.hubapi.com",
      path,
      method,
      headers: {
        Authorization: `Bearer ${ACCESS_TOKEN}`,
        "Content-Type": "application/json",
        Accept: "application/json",
        ...(bodyStr ? { "Content-Length": Buffer.byteLength(bodyStr) } : {}),
      },
    };

    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          const parsed = data ? JSON.parse(data) : {};
          if (res.statusCode >= 400) {
            const err = new Error(
              `HubSpot API error ${res.statusCode}: ${JSON.stringify(parsed)}`
            );
            err.statusCode = res.statusCode;
            err.response = parsed;
            return reject(err);
          }
          resolve(parsed);
        } catch (e) {
          reject(new Error(`Failed to parse HubSpot response: ${data}`));
        }
      });
    });

    req.on("error", reject);
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

/**
 * Retry wrapper for HubSpot API calls (handles 429 rate limiting).
 * @param {Function} fn - Async function to retry
 * @param {number} maxRetries - Maximum retry attempts
 * @returns {Promise<*>}
 */
async function withRetry(fn, maxRetries = 3) {
  let lastError;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      if (err.statusCode === 429) {
        const delay = Math.pow(2, attempt) * 1000; // exponential backoff
        console.warn(`[hubspot_deals] Rate limited (429). Retrying in ${delay}ms...`);
        await new Promise((r) => setTimeout(r, delay));
      } else {
        throw err; // Non-retryable error
      }
    }
  }
  throw lastError;
}

// ---------------------------------------------------------------------------
// Pipeline helpers
// ---------------------------------------------------------------------------

/**
 * Fetch all deal pipelines and cache them.
 * @returns {Promise<Array>} Array of pipeline objects
 */
async function getPipelines() {
  if (_pipelineCache) return _pipelineCache;
  const data = await withRetry(() =>
    hubspotRequest("GET", "/crm/v3/pipelines/deals")
  );
  _pipelineCache = data.results || [];
  return _pipelineCache;
}

/**
 * Get the pipeline ID and stage IDs for "Camelot Brokerage".
 * @returns {Promise<{pipelineId: string, stages: object}>}
 */
async function getBrokeragePipelineConfig() {
  const pipelines = await getPipelines();
  const pipeline = pipelines.find(
    (p) => p.label === BROKERAGE_PIPELINE_NAME || p.displayOrder !== undefined
  );

  if (!pipeline) {
    throw new Error(
      `Pipeline "${BROKERAGE_PIPELINE_NAME}" not found. ` +
        `Available pipelines: ${pipelines.map((p) => p.label).join(", ")}`
    );
  }

  const stages = {};
  for (const stage of pipeline.stages || []) {
    stages[stage.label] = stage.id;
  }

  return { pipelineId: pipeline.id, stages };
}

/**
 * List all pipeline stages (utility — for setup/debugging).
 */
async function listPipelineStages() {
  const config = await getBrokeragePipelineConfig();
  console.log("Brokerage Pipeline ID:", config.pipelineId);
  console.log("Stages:", JSON.stringify(config.stages, null, 2));
  return config;
}

// ---------------------------------------------------------------------------
// Company (association) helpers
// ---------------------------------------------------------------------------

/**
 * Find or create a HubSpot company for a seller/property entity.
 * @param {string} companyName
 * @returns {Promise<string>} Company ID
 */
async function findOrCreateCompany(companyName) {
  if (!companyName) return null;

  // Search for existing company
  const searchBody = {
    filterGroups: [
      {
        filters: [
          { propertyName: "name", operator: "EQ", value: companyName },
        ],
      },
    ],
    properties: ["name", "hs_object_id"],
    limit: 1,
  };

  try {
    const searchResult = await withRetry(() =>
      hubspotRequest("POST", "/crm/v3/objects/companies/search", searchBody)
    );
    if (searchResult.total > 0) {
      return searchResult.results[0].id;
    }
  } catch (err) {
    console.error("[hubspot_deals] Company search failed:", err.message);
  }

  // Create new company
  const company = await withRetry(() =>
    hubspotRequest("POST", "/crm/v3/objects/companies", {
      properties: { name: companyName },
    })
  );
  return company.id;
}

// ---------------------------------------------------------------------------
// Core API functions
// ---------------------------------------------------------------------------

/**
 * Create a new brokerage deal in HubSpot.
 *
 * @param {object} listingData - Listing/property information
 * @param {string} listingData.address          - Full property address
 * @param {string} [listingData.borough]        - Borough or county
 * @param {string} [listingData.assetType]      - Multifamily, Mixed-Use, etc.
 * @param {number} [listingData.askingPrice]    - Asking price (dollars)
 * @param {number} [listingData.units]          - Number of units
 * @param {number} [listingData.capRate]        - Cap rate (%)
 * @param {string} [listingData.sellerName]     - Seller entity name
 * @param {string} [listingData.sourcedBy]      - Agent who sourced the deal
 * @param {string} [listingData.notes]          - Additional notes
 * @returns {Promise<object>} Created deal object with id
 */
async function createBrokerageDeal(listingData) {
  const { pipelineId, stages } = await getBrokeragePipelineConfig();
  const stageId = stages[STAGE_NAMES.PROSPECT];

  if (!stageId) {
    throw new Error(
      `Stage "${STAGE_NAMES.PROSPECT}" not found in Camelot Brokerage pipeline. ` +
        `Available stages: ${Object.keys(stages).join(", ")}`
    );
  }

  const dealName = `${listingData.address}${listingData.units ? ` (${listingData.units} units)` : ""}`;

  const properties = {
    dealname: dealName,
    pipeline: pipelineId,
    dealstage: stageId,
    amount: listingData.askingPrice ? String(listingData.askingPrice) : undefined,
    description:
      `Asset Type: ${listingData.assetType || "N/A"}\n` +
      `Borough: ${listingData.borough || "N/A"}\n` +
      `Cap Rate: ${listingData.capRate ? `${listingData.capRate}%` : "N/A"}\n` +
      `Units: ${listingData.units || "N/A"}\n` +
      `Sourced By: ${listingData.sourcedBy || "Camelot Realty"}\n` +
      (listingData.notes ? `\nNotes: ${listingData.notes}` : ""),
    // Custom properties (must be created in HubSpot first)
    // "camelot_asset_type": listingData.assetType,
    // "camelot_borough": listingData.borough,
    closedate: _estimatedCloseDate(),
  };

  // Remove undefined values
  Object.keys(properties).forEach(
    (k) => properties[k] === undefined && delete properties[k]
  );

  const deal = await withRetry(() =>
    hubspotRequest("POST", "/crm/v3/objects/deals", { properties })
  );

  console.log(
    `[hubspot_deals] Created deal: ${dealName} (ID: ${deal.id}) — Stage: ${STAGE_NAMES.PROSPECT}`
  );

  // Associate with company if seller name provided
  if (listingData.sellerName) {
    try {
      const companyId = await findOrCreateCompany(listingData.sellerName);
      if (companyId) {
        await withRetry(() =>
          hubspotRequest(
            "PUT",
            `/crm/v3/objects/deals/${deal.id}/associations/companies/${companyId}/deal_to_company`,
            {}
          )
        );
        console.log(`[hubspot_deals] Associated deal with company: ${listingData.sellerName}`);
      }
    } catch (err) {
      console.error("[hubspot_deals] Company association failed:", err.message);
    }
  }

  return deal;
}

/**
 * Update a deal's pipeline stage.
 *
 * @param {string} dealId  - HubSpot deal object ID
 * @param {string} stage   - Stage name (must match STAGE_NAMES keys or values)
 * @returns {Promise<object>} Updated deal object
 */
async function updateDealStage(dealId, stage) {
  const { stages } = await getBrokeragePipelineConfig();

  // Accept both key (PROSPECT) and display name (Prospect)
  let stageId = stages[stage] || stages[STAGE_NAMES[stage]];

  if (!stageId) {
    throw new Error(
      `Stage "${stage}" not found. Valid stages: ${Object.keys(stages).join(", ")}`
    );
  }

  const updated = await withRetry(() =>
    hubspotRequest("PATCH", `/crm/v3/objects/deals/${dealId}`, {
      properties: { dealstage: stageId },
    })
  );

  console.log(`[hubspot_deals] Deal ${dealId} moved to stage: ${stage}`);
  return updated;
}

/**
 * Log an activity note (call, email, meeting) to a deal.
 *
 * @param {string} dealId   - HubSpot deal object ID
 * @param {string} note     - Activity note text
 * @param {object} [options] - Optional metadata
 * @param {string} [options.activityType] - "call" | "email" | "meeting" | "note" (default: "note")
 * @param {string} [options.ownerId]      - HubSpot owner user ID
 * @returns {Promise<object>} Created engagement/note object
 */
async function logActivity(dealId, note, options = {}) {
  const { activityType = "note", ownerId } = options;

  // Use HubSpot Notes API (v3)
  const noteBody = {
    properties: {
      hs_note_body: note,
      hs_timestamp: new Date().toISOString(),
      ...(ownerId ? { hubspot_owner_id: ownerId } : {}),
    },
    associations: [
      {
        to: { id: dealId },
        types: [
          {
            associationCategory: "HUBSPOT_DEFINED",
            associationTypeId: 214, // Note to Deal association type
          },
        ],
      },
    ],
  };

  const result = await withRetry(() =>
    hubspotRequest("POST", "/crm/v3/objects/notes", noteBody)
  );

  console.log(`[hubspot_deals] Logged ${activityType} note to deal ${dealId} (Note ID: ${result.id})`);
  return result;
}

/**
 * Get all deals in the Camelot Brokerage pipeline with their current stages.
 * @param {string} [stage] - Optional: filter by stage name
 * @returns {Promise<Array>} Array of deal objects
 */
async function getPipelineDeals(stage = null) {
  const { pipelineId, stages } = await getBrokeragePipelineConfig();

  const filterGroups = [
    {
      filters: [
        { propertyName: "pipeline", operator: "EQ", value: pipelineId },
        ...(stage && stages[stage]
          ? [{ propertyName: "dealstage", operator: "EQ", value: stages[stage] }]
          : []),
      ],
    },
  ];

  const searchBody = {
    filterGroups,
    properties: ["dealname", "amount", "dealstage", "closedate", "description", "createdate"],
    sorts: [{ propertyName: "createdate", direction: "DESCENDING" }],
    limit: 100,
  };

  const result = await withRetry(() =>
    hubspotRequest("POST", "/crm/v3/objects/deals/search", searchBody)
  );

  return result.results || [];
}

/**
 * Get a pipeline summary formatted as a Markdown table.
 * @returns {Promise<string>} Markdown pipeline summary
 */
async function getPipelineSummary() {
  const deals = await getPipelineDeals();
  const { stages } = await getBrokeragePipelineConfig();

  // Invert stages map: id → name
  const stageById = Object.fromEntries(
    Object.entries(stages).map(([name, id]) => [id, name])
  );

  if (!deals.length) {
    return "_No active deals in Camelot Brokerage pipeline._";
  }

  const header =
    "| # | Property | Asking Price | Stage | Close Date | Created |\n" +
    "|---|----------|-------------|-------|------------|---------|\n";

  const rows = deals
    .map((d, i) => {
      const p = d.properties;
      const stageName = stageById[p.dealstage] || p.dealstage;
      const price = p.amount
        ? `$${Number(p.amount).toLocaleString()}`
        : "—";
      const closeDate = p.closedate ? p.closedate.slice(0, 10) : "—";
      const createdDate = p.createdate ? p.createdate.slice(0, 10) : "—";
      return `| ${i + 1} | ${p.dealname} | ${price} | ${stageName} | ${closeDate} | ${createdDate} |`;
    })
    .join("\n");

  return `## Camelot Brokerage Pipeline\n\n${header}${rows}`;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function _estimatedCloseDate() {
  const d = new Date();
  d.setMonth(d.getMonth() + 4); // 4 months from now as estimated close
  return d.toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  createBrokerageDeal,
  updateDealStage,
  logActivity,
  getPipelineDeals,
  getPipelineSummary,
  listPipelineStages,
  STAGE_NAMES,
};

// ---------------------------------------------------------------------------
// CLI quick test
// ---------------------------------------------------------------------------

if (require.main === module) {
  (async () => {
    try {
      console.log("=== HubSpot Brokerage Pipeline Test ===\n");
      const summary = await getPipelineSummary();
      console.log(summary);
    } catch (err) {
      console.error("Error:", err.message);
      process.exit(1);
    }
  })();
}
