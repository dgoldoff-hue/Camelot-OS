/**
 * integrations/hubspot_client.js
 * --------------------------------
 * HubSpot CRM integration for Scout Bot — Camelot Property Management Services Corp.
 *
 * Provides functions to create/update CRM objects from Scout leads:
 *   - createContact(contactData)          → upsert a contact
 *   - createCompany(companyData)          → upsert a company
 *   - createDeal(dealData, contactId, companyId) → create a deal linked to both
 *   - addLeadToCRM(lead, contacts)        → master orchestrator
 *
 * Authentication:
 *   Set HUBSPOT_ACCESS_TOKEN environment variable (Private App token).
 *
 * Pipeline / Stage:
 *   Pipeline:  "Camelot Prospects"   (must exist in your HubSpot portal)
 *   Stage:     "appointmentscheduled"
 *
 * Field mappings:
 *   company_name  → company (HubSpot company name)
 *   lead_type     → deal name prefix
 *   score         → hs_priority (low/medium/high)
 *   source_site   → hs_lead_source (custom property or built-in)
 *
 * Usage (standalone):
 *   node hubspot_client.js
 *
 * Usage (from Python via subprocess):
 *   node integrations/hubspot_client.js '{"lead": {...}, "contacts": [...]}'
 */

"use strict";

const hubspot = require("@hubspot/api-client");

// ---------------------------------------------------------------------------
// Client initialisation
// ---------------------------------------------------------------------------

/**
 * Return an authenticated HubSpot API client.
 * Throws if HUBSPOT_ACCESS_TOKEN is not set.
 * @returns {hubspot.Client}
 */
function getClient() {
  const token = process.env.HUBSPOT_ACCESS_TOKEN;
  if (!token) {
    throw new Error(
      "HUBSPOT_ACCESS_TOKEN environment variable is not set. " +
        "Create a HubSpot Private App and set the token."
    );
  }
  return new hubspot.Client({ accessToken: token });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Map a Scout lead score (0–100) to a HubSpot priority label.
 * @param {number} score
 * @returns {"LOW"|"MEDIUM"|"HIGH"}
 */
function scoreToPriority(score) {
  if (score >= 70) return "HIGH";
  if (score >= 40) return "MEDIUM";
  return "LOW";
}

/**
 * Return the current ISO-8601 timestamp string (UTC).
 * @returns {string}
 */
function nowISO() {
  return new Date().toISOString();
}

/**
 * Safely stringify a value for logging — truncates long strings.
 * @param {*} value
 * @param {number} [maxLen=200]
 * @returns {string}
 */
function safeStr(value, maxLen = 200) {
  const s = typeof value === "string" ? value : JSON.stringify(value);
  return s.length > maxLen ? s.slice(0, maxLen) + "…" : s;
}

// ---------------------------------------------------------------------------
// createContact
// ---------------------------------------------------------------------------

/**
 * Create or update a HubSpot contact.
 *
 * Uses the contacts upsert-by-email endpoint so duplicate contacts are
 * avoided. If no email is provided a plain create is attempted.
 *
 * @param {Object} contactData
 * @param {string} [contactData.email]
 * @param {string} [contactData.first_name]
 * @param {string} [contactData.last_name]
 * @param {string} [contactData.name]         full name fallback
 * @param {string} [contactData.title]        job title
 * @param {string[]} [contactData.phone]      array of phone strings
 * @param {string} [contactData.linkedin_url]
 * @param {string} [contactData.company]
 * @param {string} [contactData.source]       e.g. "Apollo.io"
 * @returns {Promise<string|null>}            HubSpot contact ID or null on error
 */
async function createContact(contactData) {
  const client = getClient();

  // Resolve first/last name from name parts or split full name
  let firstName = contactData.first_name || "";
  let lastName = contactData.last_name || "";
  if (!firstName && !lastName && contactData.name) {
    const parts = contactData.name.trim().split(/\s+/);
    firstName = parts[0] || "";
    lastName = parts.slice(1).join(" ") || "";
  }

  const phone =
    Array.isArray(contactData.phone) && contactData.phone.length > 0
      ? contactData.phone[0]
      : contactData.phone || "";

  const properties = {
    firstname: firstName,
    lastname: lastName,
    jobtitle: contactData.title || "",
    phone: phone,
    email: contactData.email || "",
    linkedinbio: contactData.linkedin_url || "",
    company: contactData.company || "",
    // Custom Scout properties (must be created in HubSpot first)
    scout_source: contactData.source || "",
    hs_lead_source: contactData.source || "Scout Bot",
  };

  // Remove empty-string properties to avoid overwriting existing data
  Object.keys(properties).forEach((k) => {
    if (properties[k] === "") delete properties[k];
  });

  try {
    if (properties.email) {
      // Upsert by email — create if not exists, update if exists
      const response = await client.crm.contacts.basicApi.create({
        properties,
        associations: [],
      });
      const id = response.id;
      console.log(
        `[HubSpot] Contact upserted: ${safeStr(properties.email)} → id=${id}`
      );
      return id;
    } else {
      // No email — plain create
      const response = await client.crm.contacts.basicApi.create({
        properties,
        associations: [],
      });
      const id = response.id;
      console.log(
        `[HubSpot] Contact created (no email): ${firstName} ${lastName} → id=${id}`
      );
      return id;
    }
  } catch (err) {
    // 409 Conflict = contact already exists with this email; fetch existing ID
    if (err.code === 409 || (err.body && err.body.status === "CONFLICT")) {
      try {
        const existing = await client.crm.contacts.basicApi.getById(
          contactData.email,
          ["email"],
          undefined,
          undefined,
          undefined,
          true // idProperty = email
        );
        console.log(
          `[HubSpot] Contact already exists: ${contactData.email} → id=${existing.id}`
        );
        return existing.id;
      } catch (fetchErr) {
        console.error(
          `[HubSpot] Error fetching existing contact ${contactData.email}: ${fetchErr.message}`
        );
        return null;
      }
    }
    console.error(
      `[HubSpot] createContact error for ${safeStr(contactData.email)}: ${err.message}`
    );
    return null;
  }
}

// ---------------------------------------------------------------------------
// createCompany
// ---------------------------------------------------------------------------

/**
 * Create or update a HubSpot company.
 *
 * Searches for an existing company by name before creating to avoid
 * duplicates. Updates properties if found.
 *
 * @param {Object} companyData
 * @param {string} companyData.company_name   required
 * @param {string} [companyData.region]
 * @param {string} [companyData.raw_location]
 * @param {string} [companyData.source_site]
 * @param {string} [companyData.link]         company website / source URL
 * @param {string} [companyData.phone]
 * @param {string} [companyData.email]
 * @param {string} [companyData.category]
 * @param {number} [companyData.score]
 * @returns {Promise<string|null>}            HubSpot company ID or null on error
 */
async function createCompany(companyData) {
  const client = getClient();

  const name = (companyData.company_name || companyData.title || "").trim();
  if (!name) {
    console.warn("[HubSpot] createCompany: no company name provided.");
    return null;
  }

  // Build properties
  const phone =
    Array.isArray(companyData.phone)
      ? companyData.phone[0] || ""
      : companyData.phone || "";

  const email =
    Array.isArray(companyData.email)
      ? companyData.email[0] || ""
      : companyData.email || "";

  const properties = {
    name: name,
    city: companyData.raw_location || companyData.region || "",
    state: companyData.region || "",
    phone: phone,
    // Use link as website if it looks like a company URL, else omit
    website: (() => {
      const link = companyData.link || "";
      const known = [
        "bizbuysell.com", "bizquest.com", "loopnet.com",
        "indeed.com", "ziprecruiter.com", "nyc.gov",
      ];
      return known.some((s) => link.includes(s)) ? "" : link;
    })(),
    description: companyData.post_description
      ? companyData.post_description.slice(0, 500)
      : "",
    // Custom Scout properties
    scout_source_site: companyData.source_site || "",
    scout_category: companyData.category || "",
    scout_score: String(companyData.score || 0),
    hs_lead_source: companyData.source_site || "Scout Bot",
  };

  Object.keys(properties).forEach((k) => {
    if (properties[k] === "" || properties[k] == null) delete properties[k];
  });

  try {
    // Search for existing company by name
    const searchResp = await client.crm.companies.searchApi.doSearch({
      filterGroups: [
        {
          filters: [
            {
              propertyName: "name",
              operator: "EQ",
              value: name,
            },
          ],
        },
      ],
      properties: ["name"],
      limit: 1,
    });

    if (searchResp.results && searchResp.results.length > 0) {
      const existingId = searchResp.results[0].id;
      // Update existing company
      await client.crm.companies.basicApi.update(existingId, { properties });
      console.log(
        `[HubSpot] Company updated: "${name}" → id=${existingId}`
      );
      return existingId;
    }

    // Create new company
    const response = await client.crm.companies.basicApi.create({
      properties,
      associations: [],
    });
    console.log(`[HubSpot] Company created: "${name}" → id=${response.id}`);
    return response.id;
  } catch (err) {
    console.error(`[HubSpot] createCompany error for "${name}": ${err.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// createDeal
// ---------------------------------------------------------------------------

/**
 * Create a HubSpot deal linked to a contact and/or company.
 *
 * @param {Object} dealData
 * @param {string} dealData.deal_name         deal title
 * @param {string} [dealData.lead_type]       "Acquisition" | "Management mandate" | etc.
 * @param {number} [dealData.score]           lead quality score 0–100
 * @param {string} [dealData.source_site]     e.g. "BizBuySell"
 * @param {string} [dealData.region]          e.g. "NY"
 * @param {string} [dealData.link]            source URL
 * @param {string} [dealData.post_description]
 * @param {string|null} contactId             HubSpot contact ID to associate
 * @param {string|null} companyId             HubSpot company ID to associate
 * @returns {Promise<string|null>}            HubSpot deal ID or null on error
 */
async function createDeal(dealData, contactId, companyId) {
  const client = getClient();

  const dealName = [
    dealData.lead_type || "Scout Lead",
    "—",
    dealData.deal_name || "Unknown Company",
  ].join(" ");

  const properties = {
    dealname: dealName,
    pipeline: "default",                // Will be updated to Camelot Prospects pipeline ID
    dealstage: "appointmentscheduled",
    hs_priority: scoreToPriority(dealData.score || 0),
    description: dealData.post_description
      ? dealData.post_description.slice(0, 1000)
      : "",
    // Custom properties — must be created in HubSpot portal as deal properties
    scout_source_site: dealData.source_site || "",
    scout_region: dealData.region || "",
    scout_lead_type: dealData.lead_type || "",
    scout_score: String(dealData.score || 0),
    scout_source_url: dealData.link || "",
    scout_run_date: nowISO(),
  };

  Object.keys(properties).forEach((k) => {
    if (properties[k] === "" || properties[k] == null) delete properties[k];
  });

  // Build associations array
  const associations = [];
  if (contactId) {
    associations.push({
      to: { id: contactId },
      types: [
        {
          associationCategory: "HUBSPOT_DEFINED",
          associationTypeId: 3, // deal → contact
        },
      ],
    });
  }
  if (companyId) {
    associations.push({
      to: { id: companyId },
      types: [
        {
          associationCategory: "HUBSPOT_DEFINED",
          associationTypeId: 5, // deal → company
        },
      ],
    });
  }

  try {
    // Look up the "Camelot Prospects" pipeline ID
    let pipelineId = "default";
    try {
      const pipelines = await client.crm.pipelines.pipelinesApi.getAll("deals");
      const camelotPipeline = pipelines.results.find(
        (p) =>
          p.label === "Camelot Prospects" ||
          p.displayOrder != null // fallback: first non-default
      );
      if (camelotPipeline) {
        pipelineId = camelotPipeline.id;
        // Resolve stage ID within this pipeline
        const stages = camelotPipeline.stages || [];
        const targetStage = stages.find(
          (s) =>
            s.label === "appointmentscheduled" ||
            s.metadata?.probability != null
        );
        if (targetStage) {
          properties.dealstage = targetStage.id;
        }
        properties.pipeline = pipelineId;
      }
    } catch (pipelineErr) {
      console.warn(
        `[HubSpot] Could not resolve pipeline: ${pipelineErr.message}. Using default.`
      );
    }

    const response = await client.crm.deals.basicApi.create({
      properties,
      associations,
    });

    console.log(
      `[HubSpot] Deal created: "${dealName}" → id=${response.id} ` +
        `(pipeline=${pipelineId}, stage=${properties.dealstage})`
    );
    return response.id;
  } catch (err) {
    console.error(`[HubSpot] createDeal error for "${dealName}": ${err.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// addLeadToCRM  (master orchestrator)
// ---------------------------------------------------------------------------

/**
 * Push a full Scout lead (with contacts) into HubSpot CRM.
 *
 * Creates:
 *   1. One Company record (from lead.company_name)
 *   2. One Contact per enriched contact (from contacts array)
 *   3. One Deal linked to the first contact + the company
 *
 * @param {Object} lead      Scout lead dict (camelCase keys OK; snake_case used here)
 * @param {Object[]} contacts Enriched contacts array (may be empty)
 * @returns {Promise<{companyId: string|null, contactIds: string[], dealId: string|null}>}
 */
async function addLeadToCRM(lead, contacts = []) {
  const company_name =
    lead.company_name || lead.title || "Unknown Company";

  console.log(
    `[HubSpot] addLeadToCRM: company="${company_name}" ` +
      `contacts=${contacts.length} score=${lead.score || 0}`
  );

  // 1. Create / update company
  const companyId = await createCompany({
    company_name,
    region: lead.region || "",
    raw_location: lead.raw_location || "",
    source_site: lead.source_site || "",
    link: lead.link || "",
    phone: lead.phone || [],
    email: lead.email || [],
    category: lead.category || "",
    score: lead.score || 0,
    post_description: lead.post_description || "",
  });

  // 2. Create contacts
  const contactIds = [];
  const contactsToProcess =
    contacts.length > 0 ? contacts : _buildContactsFromLead(lead);

  for (const contact of contactsToProcess) {
    const cid = await createContact({
      ...contact,
      company: company_name,
    });
    if (cid) contactIds.push(cid);
  }

  // 3. Create deal (associated to first contact + company)
  const primaryContactId = contactIds.length > 0 ? contactIds[0] : null;

  const dealId = await createDeal(
    {
      deal_name: company_name,
      lead_type: lead.lead_type || lead.category || "Scout Lead",
      score: lead.score || 0,
      source_site: lead.source_site || "",
      region: lead.region || "",
      link: lead.link || "",
      post_description: lead.post_description || "",
    },
    primaryContactId,
    companyId
  );

  // 4. Associate any additional contacts to the deal
  if (dealId && contactIds.length > 1) {
    const client = getClient();
    for (const cid of contactIds.slice(1)) {
      try {
        await client.crm.associations.v4.basicApi.create(
          "deals",
          dealId,
          "contacts",
          cid,
          [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 3 }]
        );
      } catch (assocErr) {
        console.warn(
          `[HubSpot] Could not associate contact ${cid} to deal ${dealId}: ${assocErr.message}`
        );
      }
    }
  }

  const result = { companyId, contactIds, dealId };
  console.log(
    `[HubSpot] addLeadToCRM complete for "${company_name}": ` +
      JSON.stringify(result)
  );
  return result;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Build a minimal contacts array from lead-level contact fields
 * when no enriched contacts are available.
 * @param {Object} lead
 * @returns {Object[]}
 */
function _buildContactsFromLead(lead) {
  const contacts = [];
  const emails = Array.isArray(lead.email) ? lead.email : [lead.email].filter(Boolean);
  const phones = Array.isArray(lead.phone) ? lead.phone : [lead.phone].filter(Boolean);

  if (emails.length > 0 || phones.length > 0) {
    contacts.push({
      email: emails[0] || "",
      phone: phones,
      name: lead.author || lead.company_name || "",
      company: lead.company_name || "",
      source: lead.source_site || "Scout Bot",
    });
  }
  return contacts;
}

// ---------------------------------------------------------------------------
// Batch push
// ---------------------------------------------------------------------------

/**
 * Push an array of Scout leads to HubSpot.
 * Processes leads sequentially to avoid API rate limits.
 *
 * @param {Object[]} leads   Array of Scout lead dicts (with contacts populated)
 * @returns {Promise<Object[]>} Array of result objects {lead_title, companyId, contactIds, dealId}
 */
async function pushLeadsToCRM(leads) {
  const results = [];
  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];
    const contacts = lead.contacts || [];
    console.log(
      `[HubSpot] Pushing lead ${i + 1}/${leads.length}: "${lead.company_name || lead.title}"`
    );
    try {
      const result = await addLeadToCRM(lead, contacts);
      results.push({ lead_title: lead.title, ...result });
    } catch (err) {
      console.error(
        `[HubSpot] Error pushing lead "${lead.title}": ${err.message}`
      );
      results.push({
        lead_title: lead.title,
        companyId: null,
        contactIds: [],
        dealId: null,
        error: err.message,
      });
    }
    // Small delay between leads to avoid HubSpot rate limits (10 req/s limit)
    await new Promise((r) => setTimeout(r, 200));
  }
  return results;
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------
// Called from Python via:
//   node integrations/hubspot_client.js '<json_payload>'
//
// Payload shape:
//   { "leads": [ {...lead with contacts...}, ... ] }
// OR for a single lead:
//   { "lead": {...}, "contacts": [...] }

if (require.main === module) {
  (async () => {
    const raw = process.argv[2];
    if (!raw) {
      console.error(
        "Usage: node hubspot_client.js '<json_payload>'\n" +
          "Payload: { leads: [...] } or { lead: {...}, contacts: [...] }"
      );
      process.exit(1);
    }

    let payload;
    try {
      payload = JSON.parse(raw);
    } catch (e) {
      console.error("Invalid JSON payload:", e.message);
      process.exit(1);
    }

    try {
      let results;
      if (payload.leads) {
        results = await pushLeadsToCRM(payload.leads);
      } else if (payload.lead) {
        const r = await addLeadToCRM(payload.lead, payload.contacts || []);
        results = [r];
      } else {
        console.error("Payload must have 'leads' array or 'lead' object.");
        process.exit(1);
      }

      // Output results as JSON so Python subprocess can parse them
      process.stdout.write(JSON.stringify({ success: true, results }, null, 2));
      process.exit(0);
    } catch (err) {
      process.stderr.write(
        JSON.stringify({ success: false, error: err.message })
      );
      process.exit(1);
    }
  })();
}

// ---------------------------------------------------------------------------
// Exports (for use as a Node.js module)
// ---------------------------------------------------------------------------
module.exports = {
  createContact,
  createCompany,
  createDeal,
  addLeadToCRM,
  pushLeadsToCRM,
};
