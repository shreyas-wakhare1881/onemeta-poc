import { Room, createLocalTracks as lkCreateLocalTracks, LocalTrack, type CreateLocalTracksOptions, type LocalTrackPublication } from 'livekit-client';

/**
 * Creates and returns a new LiveKit Room instance with standard configurations.
 */
export function createRoom(): Room {
  return new Room();
}

/**
 * Asynchronously connects a given Room instance to the LiveKit server.
 */
export async function connectRoom(room: Room, url: string, token: string): Promise<Room> {
  await room.connect(url, token);
  return room;
}

/**
 * Asynchronously disconnects a given Room instance.
 */
export async function disconnectRoom(room: Room): Promise<void> {
  await room.disconnect();
}

/**
 * Creates local media tracks (video and/or audio).
 */
export async function createLocalTracks(options?: CreateLocalTracksOptions): Promise<LocalTrack[]> {
  return await lkCreateLocalTracks(options);
}

/**
 * Publishes a list of local tracks to the given room.
 */
export async function publishLocalTracks(room: Room, tracks: LocalTrack[]): Promise<LocalTrackPublication[]> {
  const publications: LocalTrackPublication[] = [];
  for (const track of tracks) {
    const pub = await room.localParticipant.publishTrack(track);
    publications.push(pub);
  }
  return publications;
}

/**
 * Unpublishes and stops a list of local tracks from the given room.
 */
export async function stopLocalTracks(room: Room, tracks: LocalTrack[]): Promise<void> {
  for (const track of tracks) {
    try {
      await room.localParticipant.unpublishTrack(track);
      track.stop();
    } catch (err) {
      console.error('Failed to unpublish/stop track:', err);
    }
  }
}
