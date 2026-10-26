import { act, cleanup, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { $changeEventsAvailable, $petChange, notifyPetChanged } from '@/store/live-sync'
import { $petInfo, type PetInfo, setPetInfo } from '@/store/pet'
import { $activeGatewayProfile } from '@/store/profile'
import { $gatewayState } from '@/store/session'
import { setDocumentHidden } from '@/test/window-state'

import { FloatingPet } from './floating-pet'

const { requestGateway } = vi.hoisted(() => ({
  requestGateway: vi.fn<(method: string, params?: Record<string, unknown>) => Promise<unknown>>()
}))

vi.mock('@/app/gateway/hooks/use-gateway-request', () => ({ useGatewayRequest: () => ({ requestGateway }) }))
vi.mock('@/app/hooks/use-route-overlay-active', () => ({ useRouteOverlayActive: () => false }))
vi.mock('@/themes/context', () => ({ useTheme: () => ({ resolvedMode: 'dark' }) }))
vi.mock('@/components/chat/vibe-hearts', () => ({ PetHeartField: () => null }))
vi.mock('./pet-bubble', () => ({ PetBubble: () => null }))
vi.mock('./pet-sprite', () => ({
  PetSprite: ({ info }: { info: PetInfo }) => <div data-testid="pet-sprite">{info.slug}</div>,
  roamWalkRow: () => ({ mirror: false, row: 'idle' })
}))
vi.mock('./use-pet-roam', () => ({ usePetRoam: () => undefined }))
vi.mock('@/store/pet-overlay', async original => ({
  ...(await original<Record<string, unknown>>()),
  initPetOverlayBridge: () => undefined,
  restorePetOverlay: () => undefined
}))

const PET: PetInfo = {
  enabled: true,
  slug: 'boba',
  spritesheetBase64: 'sheet',
  spritesheetRevision: 'revision-1',
  frameW: 16,
  frameH: 16,
  framesPerState: 1,
  stateRows: ['idle'],
  scale: 1
}

async function settle(run: () => void = () => undefined) {
  await act(async () => {
    run()
  })
}

async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

beforeEach(() => {
  vi.useFakeTimers()
  requestGateway.mockReset()
  setDocumentHidden(false)
  $activeGatewayProfile.set('default')
  $changeEventsAvailable.set(true)
  $petChange.set({ tick: 0 })
  setPetInfo({ enabled: false })
  $gatewayState.set('open')
})

afterEach(() => {
  cleanup()
  $gatewayState.set('closed')
  $petChange.set({ tick: 0 })
  $changeEventsAvailable.set(false)
  $activeGatewayProfile.set('default')
  setPetInfo({ enabled: false })
  setDocumentHidden(false)
  vi.useRealTimers()
})

describe('FloatingPet reconciliation after disabled events', () => {
  it('recovers through startup retries, focus and the backstop without repeating sprite downloads', async () => {
    for (const recovery of ['startup', 'focus', 'backstop', 'legacy'] as const) {
      const legacy = recovery === 'legacy'
      let enabled = true
      let downloads = 0

      $changeEventsAvailable.set(!legacy)
      $petChange.set({ tick: 0 })
      setPetInfo({ enabled: false })
      requestGateway.mockReset().mockImplementation(async (method, params) => {
        if (method === 'pet.info.meta') {
          if (legacy) {
            throw new Error('Unknown method: pet.info.meta')
          }

          return { ...PET, enabled, spritesheetBase64: undefined }
        }

        expect(method).toBe('pet.info')

        if (!enabled) {
          return { enabled: false }
        }

        if (params?.knownRevision === PET.spritesheetRevision) {
          return { ...PET, spritesheetBase64: undefined, spritesheetUnchanged: true }
        }

        downloads += 1

        return { ...PET }
      })

      const view = render(<FloatingPet />)
      await settle()
      expect(screen.getByTestId('pet-sprite').textContent).toBe('boba')
      enabled = false
      await settle(() => notifyPetChanged({ enabled: false }))

      if (legacy) {
        // Older gateways do not send hints; discover the disabled state by polling.
        await advance(15_000)
      }

      expect(screen.queryByTestId('pet-sprite')).toBeNull()

      if (recovery === 'backstop') {
        await advance(8_000)
        expect(screen.queryByTestId('pet-sprite')).toBeNull()
      }

      enabled = true

      if (recovery === 'focus') {
        await settle(() => window.dispatchEvent(new Event('focus')))
      } else {
        await advance(recovery === 'backstop' ? 7_000 : legacy ? 3_000 : 1_000)
      }

      expect(screen.getByTestId('pet-sprite').textContent, recovery).toBe('boba')
      expect(downloads, recovery).toBe(2)

      const calls = requestGateway.mock.calls.length
      await advance(45_000)
      expect(screen.getByTestId('pet-sprite').textContent, recovery).toBe('boba')
      expect(downloads, recovery).toBe(2)
      expect(requestGateway.mock.calls.length - calls, recovery).toBe(legacy ? 6 : 3)

      view.unmount()
      const afterUnmount = requestGateway.mock.calls.length
      await settle(() => window.dispatchEvent(new Event('focus')))
      await advance(30_000)
      expect(requestGateway.mock.calls.length).toBe(afterUnmount)
    }
  })

  it('cancels hidden-profile reads and stale metadata fallbacks when the gateway profile switches', async () => {
    let resolveOld!: (info: PetInfo) => void
    let resolveNew!: (info: PetInfo) => void
    let rejectMeta!: (error: Error) => void
    let pendingOld = false
    let pendingMeta = false
    const oldReply = new Promise<PetInfo>(resolve => (resolveOld = resolve))
    const newReply = new Promise<PetInfo>(resolve => (resolveNew = resolve))
    const metaReply = new Promise<PetInfo>((_resolve, reject) => (rejectMeta = reject))

    requestGateway.mockImplementation(async (method, params) => {
      if (method === 'pet.info.meta') {
        return pendingMeta && params?.profile === 'new-profile' ? metaReply : { ...PET, slug: params?.profile }
      }

      if (params?.profile === 'default' && pendingOld) {
        return oldReply
      }

      if (params?.profile === 'new-profile') {
        return newReply
      }

      return { ...PET, slug: params?.profile }
    })

    render(<FloatingPet />)
    await settle()
    expect(screen.getByTestId('pet-sprite').textContent).toBe('default')

    pendingOld = true
    await settle(() => notifyPetChanged({ enabled: false }))
    await settle(() => window.dispatchEvent(new Event('focus')))
    expect(screen.queryByTestId('pet-sprite')).toBeNull()

    await settle(() => $activeGatewayProfile.set('new-profile'))
    expect(requestGateway).toHaveBeenCalledWith('pet.info', { knownRevision: undefined, profile: 'new-profile' })
    await settle(() => resolveOld({ ...PET, slug: 'default' }))
    expect(screen.queryByTestId('pet-sprite')).toBeNull()
    await settle(() => resolveNew({ ...PET, slug: 'new-profile' }))
    expect(screen.getByTestId('pet-sprite').textContent).toBe('new-profile')

    pendingMeta = true
    await settle(() => window.dispatchEvent(new Event('focus')))
    await settle(() => $activeGatewayProfile.set('third-profile'))
    await settle()
    expect(screen.getByTestId('pet-sprite').textContent).toBe('third-profile')
    const calls = requestGateway.mock.calls.length
    await settle(() => rejectMeta(new Error('Old profile disconnected')))
    expect(requestGateway.mock.calls.length).toBe(calls)
    expect($petInfo.get().slug).toBe('third-profile')
  })
})
