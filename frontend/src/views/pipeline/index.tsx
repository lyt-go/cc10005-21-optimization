import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/pipeline'

type StageNode = {
  stage: string
  module: string
  rule_id: string
  rule_version: number
  output_value: string | null
  reused: boolean
  source_node_id: number | null
  node_id: number
}

type FieldResult = {
  field_name: string
  parser_rule: string
  parser_version: number
  clean_rule: string
  clean_version: number
  parsed_value: string | null
  cleaned_value: string | null
  stages: StageNode[]
}

type ResultRow = {
  record_id: number
  source: string
  raw_payload: Record<string, string>
  fields: Record<string, FieldResult>
}

type RuleVersion = {
  rule_id: string
  version: number
  module: string
  field_name: string
  config: Record<string, unknown>
  references: string[]
  status: string
  confirmed_seq: number | null
  created_at: string
  confirmed_at: string | null
}

type Batch = {
  batch_id: number
  status: string
  trigger_note: string
  affected_fields: string[]
  reused_fields: string[]
  node_summary: {
    fields?: Record<string, { created: number; reused: number }>
    stages?: Record<string, { created: number; reused: number }>
  }
  error_message: string | null
  created_at: string
  finished_at: string | null
}

type GraphNode = {
  field_name: string
  references: string[]
  parser_rule: string
  parser_version: number
  clean_rule: string
  clean_version: number
}

type RuleDraft = {
  module: 'parser' | 'dataclean'
  rule_id: string
  field_name: string
  references: string
  // parser
  xpath: string
  regex: string
  default: string
  convert_type: string
  precision: string
  // dataclean
  enabled_html: boolean
  enabled_special: boolean
  whitespace_mode: string
  max_length: string
  enabled_encoding: boolean
  sensitive_words: string
}

const STAGE_LABELS: Record<string, string> = {
  extract: '抽取',
  convert: '类型转换',
  html_clean: 'HTML清理',
  special_char: '特殊字符过滤',
  whitespace: '空白规整',
  truncate: '长度截断',
  encoding_fix: '编码纠错',
  mask: '敏感词脱敏',
}

function defaultDraft(module: 'parser' | 'dataclean'): RuleDraft {
  if (module === 'parser') {
    return {
      module, rule_id: 'PARS-0002', field_name: '价格', references: '',
      xpath: '//item/price/text()', regex: String.raw`\d[\d,\.]*`, default: '0',
      convert_type: 'number', precision: '4',
      enabled_html: false, enabled_special: true, whitespace_mode: 'collapse',
      max_length: '0', enabled_encoding: false, sensitive_words: '',
    }
  }
  return {
    module, rule_id: 'DATA-0002', field_name: '价格', references: '',
    xpath: '', regex: '', default: '', convert_type: 'text', precision: '',
    enabled_html: false, enabled_special: true, whitespace_mode: 'collapse',
    max_length: '5', enabled_encoding: false, sensitive_words: '1299',
  }
}

function buildPayload(draft: RuleDraft) {
  if (draft.module === 'parser') {
    return {
      module: draft.module,
      rule_id: draft.rule_id,
      field_name: draft.field_name,
      references: draft.references.split(',').map((s) => s.trim()).filter(Boolean),
      config: {
        xpath: draft.xpath,
        regex: draft.regex,
        default: draft.default,
        convert_type: draft.convert_type,
        precision: draft.precision === '' ? null : Number(draft.precision),
      },
    }
  }
  return {
    module: draft.module,
    rule_id: draft.rule_id,
    field_name: draft.field_name,
    references: draft.references.split(',').map((s) => s.trim()).filter(Boolean),
    config: {
      enabled_html: draft.enabled_html,
      enabled_special: draft.enabled_special,
      whitespace_mode: draft.whitespace_mode,
      max_length: Number(draft.max_length || 0),
      enabled_encoding: draft.enabled_encoding,
      sensitive_words: draft.sensitive_words.split(',').map((s) => s.trim()).filter(Boolean),
    },
  }
}

export default function PipelineView() {
  const [graph, setGraph] = useState<{ nodes: GraphNode[]; edges: { from: string; to: string }[] }>({ nodes: [], edges: [] })
  const [rows, setRows] = useState<ResultRow[]>([])
  const [versions, setVersions] = useState<RuleVersion[]>([])
  const [batches, setBatches] = useState<Batch[]>([])
  const [fieldFilter, setFieldFilter] = useState('')
  const [drafts, setDrafts] = useState<RuleDraft[]>([defaultDraft('parser'), defaultDraft('dataclean')])
  const [triggerNote, setTriggerNote] = useState('PARS-0002 修改字段转换与清洗规则')
  const [expanded, setExpanded] = useState<string>('')
  const [errorMessage, setErrorMessage] = useState('')
  const [infoMessage, setInfoMessage] = useState('')

  const fields = useMemo(() => graph.nodes.map((n) => n.field_name), [graph])

  const reload = useCallback(async () => {
    setErrorMessage('')
    try {
      const query = fieldFilter ? `?field_name=${encodeURIComponent(fieldFilter)}` : ''
      const [graphRes, resultRes, versionRes, batchRes] = await Promise.all([
        request(`${ENDPOINT}/graph`),
        request(`${ENDPOINT}/results${query}`),
        request(`${ENDPOINT}/rules/versions`),
        request(`${ENDPOINT}/batches`),
      ])
      if (!graphRes.ok || !resultRes.ok || !versionRes.ok || !batchRes.ok) {
        throw new Error('流水线数据读取失败')
      }
      setGraph(await graphRes.json())
      const resultPayload = await resultRes.json()
      setRows(resultPayload.items ?? [])
      setVersions((await versionRes.json()).items ?? [])
      setBatches((await batchRes.json()).items ?? [])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流水线数据读取失败')
    }
  }, [fieldFilter])

  useEffect(() => {
    void reload()
  }, [reload])

  function patchDraft(index: number, patch: Partial<RuleDraft>) {
    setDrafts((prev) => prev.map((draft, i) => (i === index ? { ...draft, ...patch } : draft)))
  }

  async function submitBackfill() {
    setErrorMessage('')
    setInfoMessage('')
    try {
      const response = await request(`${ENDPOINT}/backfill`, {
        method: 'POST',
        body: JSON.stringify({ trigger_note: triggerNote, rules: drafts.map(buildPayload) }),
      })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(payload.detail || '回填失败，批次已回滚')
      }
      const batch = payload.batch as Batch
      const created = Object.values(batch.node_summary.stages ?? {}).reduce((sum, s) => sum + s.created, 0)
      const reused = Object.values(batch.node_summary.stages ?? {}).reduce((sum, s) => sum + s.reused, 0)
      setInfoMessage(
        `批次 #${batch.batch_id} 已切换新版本：受影响字段 ${batch.affected_fields.join('、') || '无'}，` +
        `新建节点 ${created} 个，复用节点 ${reused} 个`,
      )
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '回填失败')
    }
  }

  return (
    <section className="page" data-module="pipeline">
      <header className="page-head">
        <div>
          <h2>解析清洗流水线</h2>
          <p className="page-desc">
            按字段依赖图缩小重算范围：只失效引用该字段的解析结果与去噪步骤，未变化的类型转换与敏感词脱敏继续复用；
            回填先构建可切换新版本，全部成功后原子替换当前结果，历史版本永久保留。
          </p>
        </div>
      </header>

      {errorMessage && <p className="error-text">{errorMessage}</p>}
      {infoMessage && <p className="page-desc" style={{ color: '#1a7f37' }}>{infoMessage}</p>}

      <h3>字段依赖图</h3>
      <table className="data-table">
        <thead>
          <tr><th>目标字段</th><th>引用字段</th><th>解析规则</th><th>去噪规则</th><th>依赖边</th></tr>
        </thead>
        <tbody>
          {graph.nodes.map((node) => (
            <tr key={node.field_name}>
              <td>{node.field_name}</td>
              <td>{node.references.length ? node.references.join('、') : '—'}</td>
              <td>{node.parser_rule} v{node.parser_version}</td>
              <td>{node.clean_rule} v{node.clean_version}</td>
              <td>
                {graph.edges.filter((e) => e.from === node.field_name)
                  .map((e) => `${e.from} → ${e.to}`)
                  .join('；') || '—'}
              </td>
            </tr>
          ))}
          {graph.nodes.length === 0 && (
            <tr><td className="empty-state" colSpan={5}>依赖图尚未生成</td></tr>
          )}
        </tbody>
      </table>

      <h3>规则批次回填</h3>
      <div className="filter-bar" style={{ flexWrap: 'wrap' }}>
        <label className="filter-item">
          <span>批次说明</span>
          <input value={triggerNote} onChange={(e) => setTriggerNote(e.target.value)} style={{ minWidth: 260 }} />
        </label>
      </div>
      {drafts.map((draft, index) => (
        <article key={index} className="stat-card" style={{ marginBottom: 12 }}>
          <div className="filter-bar" style={{ flexWrap: 'wrap' }}>
            <label className="filter-item">
              <span>模块</span>
              <select value={draft.module} onChange={(e) => patchDraft(index, { module: e.target.value as 'parser' | 'dataclean' })}>
                <option value="parser">字段解析器</option>
                <option value="dataclean">数据去噪</option>
              </select>
            </label>
            <label className="filter-item">
              <span>规则编号</span>
              <input value={draft.rule_id} onChange={(e) => patchDraft(index, { rule_id: e.target.value })} />
            </label>
            <label className="filter-item">
              <span>目标字段</span>
              <input value={draft.field_name} onChange={(e) => patchDraft(index, { field_name: e.target.value })} />
            </label>
            <label className="filter-item">
              <span>引用字段(逗号分隔)</span>
              <input value={draft.references} onChange={(e) => patchDraft(index, { references: e.target.value })} />
            </label>
            {drafts.length > 1 && (
              <button className="btn ghost" type="button" onClick={() => setDrafts((prev) => prev.filter((_, i) => i !== index))}>
                移除该规则
              </button>
            )}
          </div>
          {draft.module === 'parser' ? (
            <div className="filter-bar" style={{ flexWrap: 'wrap' }}>
              <label className="filter-item">
                <span>XPath</span>
                <input value={draft.xpath} onChange={(e) => patchDraft(index, { xpath: e.target.value })} style={{ minWidth: 220 }} />
              </label>
              <label className="filter-item">
                <span>正则</span>
                <input value={draft.regex} onChange={(e) => patchDraft(index, { regex: e.target.value })} />
              </label>
              <label className="filter-item">
                <span>默认值</span>
                <input value={draft.default} onChange={(e) => patchDraft(index, { default: e.target.value })} />
              </label>
              <label className="filter-item">
                <span>类型转换</span>
                <select value={draft.convert_type} onChange={(e) => patchDraft(index, { convert_type: e.target.value })}>
                  <option value="text">text</option>
                  <option value="number">number</option>
                  <option value="integer">integer</option>
                </select>
              </label>
              <label className="filter-item">
                <span>数值精度</span>
                <input value={draft.precision} onChange={(e) => patchDraft(index, { precision: e.target.value })} style={{ width: 90 }} />
              </label>
            </div>
          ) : (
            <div className="filter-bar" style={{ flexWrap: 'wrap' }}>
              <label className="filter-item">
                <span>HTML清理</span>
                <input type="checkbox" checked={draft.enabled_html} onChange={(e) => patchDraft(index, { enabled_html: e.target.checked })} />
              </label>
              <label className="filter-item">
                <span>特殊字符过滤</span>
                <input type="checkbox" checked={draft.enabled_special} onChange={(e) => patchDraft(index, { enabled_special: e.target.checked })} />
              </label>
              <label className="filter-item">
                <span>空白规整</span>
                <select value={draft.whitespace_mode} onChange={(e) => patchDraft(index, { whitespace_mode: e.target.value })}>
                  <option value="collapse">collapse</option>
                  <option value="strip">strip</option>
                  <option value="keep">keep</option>
                </select>
              </label>
              <label className="filter-item">
                <span>截断阈值</span>
                <input value={draft.max_length} onChange={(e) => patchDraft(index, { max_length: e.target.value })} style={{ width: 90 }} />
              </label>
              <label className="filter-item">
                <span>编码纠错</span>
                <input type="checkbox" checked={draft.enabled_encoding} onChange={(e) => patchDraft(index, { enabled_encoding: e.target.checked })} />
              </label>
              <label className="filter-item">
                <span>敏感词(逗号分隔)</span>
                <input value={draft.sensitive_words} onChange={(e) => patchDraft(index, { sensitive_words: e.target.value })} style={{ minWidth: 200 }} />
              </label>
            </div>
          )}
        </article>
      ))}
      <div className="page-actions" style={{ marginBottom: 16 }}>
        <button className="btn" type="button" onClick={() => setDrafts((prev) => [...prev, defaultDraft('parser')])}>追加解析规则</button>
        <button className="btn" type="button" onClick={() => setDrafts((prev) => [...prev, defaultDraft('dataclean')])}>追加去噪规则</button>
        <button className="btn primary" type="button" onClick={() => void submitBackfill()}>提交回填并切换版本</button>
      </div>

      <h3>解析字段结果与清洗结果</h3>
      <form className="filter-bar" onSubmit={(e) => { e.preventDefault(); void reload() }}>
        <label className="filter-item">
          <span>目标字段</span>
          <select value={fieldFilter} onChange={(e) => setFieldFilter(e.target.value)}>
            <option value="">全部字段</option>
            {fields.map((f) => <option key={f} value={f}>{f}</option>)}
          </select>
        </label>
        <button className="btn" type="submit">查询</button>
      </form>
      <table className="data-table">
        <thead>
          <tr>
            <th>采集记录</th><th>目标字段</th><th>原始值</th><th>解析字段结果</th><th>清洗结果</th>
            <th>解析版本</th><th>清洗版本</th><th>阶段明细</th>
          </tr>
        </thead>
        <tbody>
          {rows.flatMap((row) =>
            Object.values(row.fields).map((field) => {
              const key = `${row.record_id}-${field.field_name}`
              const isOpen = expanded === key
              return (
                <FragmentRow
                  key={key}
                  row={row}
                  field={field}
                  isOpen={isOpen}
                  onToggle={() => setExpanded(isOpen ? '' : key)}
                />
              )
            }),
          )}
          {rows.length === 0 && (
            <tr><td className="empty-state" colSpan={8}>暂无解析/清洗结果</td></tr>
          )}
        </tbody>
      </table>

      <h3>回填批次历史</h3>
      <table className="data-table">
        <thead>
          <tr><th>批次</th><th>状态</th><th>说明</th><th>受影响字段</th><th>复用字段</th><th>新建/复用节点</th><th>时间</th></tr>
        </thead>
        <tbody>
          {batches.map((batch) => (
            <tr key={batch.batch_id}>
              <td>#{batch.batch_id}</td>
              <td>{batch.status}</td>
              <td>{batch.trigger_note}</td>
              <td>{batch.affected_fields.join('、') || '—'}</td>
              <td>{batch.reused_fields.join('、') || '—'}</td>
              <td>
                {Object.values(batch.node_summary.stages ?? {}).reduce((s, x) => s + x.created, 0)} /
                {Object.values(batch.node_summary.stages ?? {}).reduce((s, x) => s + x.reused, 0)}
              </td>
              <td>{batch.finished_at || batch.created_at}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h3>规则版本历史（原版本永久保留）</h3>
      <table className="data-table">
        <thead>
          <tr><th>规则</th><th>版本</th><th>模块</th><th>目标字段</th><th>状态</th><th>确认序号</th><th>引用</th><th>配置摘要</th><th>确认时间</th></tr>
        </thead>
        <tbody>
          {versions.map((v) => (
            <tr key={`${v.rule_id}-${v.version}`}>
              <td>{v.rule_id}</td>
              <td>v{v.version}</td>
              <td>{v.module}</td>
              <td>{v.field_name}</td>
              <td>{v.status}</td>
              <td>{v.confirmed_seq ?? '—'}</td>
              <td>{v.references.join('、') || '—'}</td>
              <td style={{ maxWidth: 320, wordBreak: 'break-all' }}>{JSON.stringify(v.config)}</td>
              <td>{v.confirmed_at || '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  )
}

function FragmentRow({
  row, field, isOpen, onToggle,
}: {
  row: ResultRow
  field: FieldResult
  isOpen: boolean
  onToggle: () => void
}) {
  return (
    <>
      <tr>
        <td>#{row.record_id}</td>
        <td>{field.field_name}</td>
        <td style={{ maxWidth: 220, wordBreak: 'break-all' }}>{row.raw_payload[field.field_name] ?? '—'}</td>
        <td>{field.parsed_value ?? '—'}</td>
        <td>{field.cleaned_value ?? '—'}</td>
        <td>{field.parser_rule} v{field.parser_version}</td>
        <td>{field.clean_rule} v{field.clean_version}</td>
        <td className="row-actions">
          <button className="link" type="button" onClick={onToggle}>{isOpen ? '收起' : '查看阶段'}</button>
        </td>
      </tr>
      {isOpen && (
        <tr>
          <td colSpan={8}>
            <table className="data-table">
              <thead>
                <tr><th>阶段</th><th>来源规则</th><th>阶段输出</th><th>节点来源</th></tr>
              </thead>
              <tbody>
                {field.stages.map((stage) => (
                  <tr key={stage.node_id}>
                    <td>{STAGE_LABELS[stage.stage] || stage.stage}</td>
                    <td>{stage.rule_id} v{stage.rule_version}</td>
                    <td>{stage.output_value ?? '—'}</td>
                    <td>{stage.reused ? `复用节点 #${stage.source_node_id}` : '本批新建'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </td>
        </tr>
      )}
    </>
  )
}
